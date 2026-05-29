package org.apache.flinkx.api.evolution.dsl

import scala.reflect.macros.blackbox

/** Scala 2 blackbox macros backing the typed-accessor overloads of the evolution DSL.
  *
  * Each macro pattern-matches the `_.field` lambda's AST to extract the selected field name. The [[added]] macro
  * additionally looks up the case class's synthetic `apply$default$N` method on the companion and rewrites the call
  * into `dsl.added(name, Some(default))` — turning what would be a runtime `AddedFieldWithoutDefaultException` into a
  * compile error. The [[renamed]] and [[transformed]] macros only need the field name.
  *
  * Compile errors are reported when:
  *   - the lambda isn't a single field select (e.g. multi-step selects, blocks with side effects);
  *   - the selected field doesn't exist on the case class;
  *   - `.added(_.field)` is used on a field without a default value.
  */
object DslMacros {

  def added[T: c.WeakTypeTag, A](
      c: blackbox.Context
  )(accessor: c.Expr[T => A]): c.Expr[EvolutionDslAt[T]] = {
    import c.universe._

    val (name, paramIdx) = fieldInfoFromAccessor(c)(accessor)
    val tType            = c.weakTypeOf[T]
    val companionSym     = tType.typeSymbol.companion

    // Look up `<init>$default$<n>` rather than `apply$default$<n>` — same convention Magnolia 2 uses. The
    // constructor default is authoritative; `apply$default` might not exist if the companion overrides `apply`.
    val defaultName   = TermName(s"<init>$$default$$${paramIdx + 1}").encodedName.toTermName
    val defaultMember = tType.companion.member(defaultName)

    if (defaultMember == NoSymbol) {
      c.abort(
        accessor.tree.pos,
        s"Field '$name' marked as added must have a default value in $tType."
      )
    }

    // Emit `dsl.added(name, Some(Companion.<init>$default$<n>))`. The default-method invocation is positional, so
    // evaluating it at runtime returns the same value Magnolia's `Param.default` would have produced.
    c.Expr[EvolutionDslAt[T]](
      q"${c.prefix.tree}.added($name, _root_.scala.Some(${Ident(companionSym)}.$defaultName))"
    )
  }

  def renamed[T: c.WeakTypeTag, A](
      c: blackbox.Context
  )(formerName: c.Expr[String], accessor: c.Expr[T => A]): c.Expr[EvolutionDslAt[T]] = {
    import c.universe._
    val (name, _) = fieldInfoFromAccessor(c)(accessor)
    c.Expr[EvolutionDslAt[T]](q"${c.prefix.tree}.renamed($formerName, $name)")
  }

  def transformed[T: c.WeakTypeTag, A, B](
      c: blackbox.Context
  )(accessor: c.Expr[T => B], mapper: c.Expr[A => B]): c.Expr[EvolutionDslAt[T]] = {
    import c.universe._
    val (name, _) = fieldInfoFromAccessor(c)(accessor)
    c.Expr[EvolutionDslAt[T]](q"${c.prefix.tree}.transformed($name, $mapper)")
  }

  /** Pattern-match the lambda's AST to extract the selected field name and its 0-based position in the case class's
    * parameter list. Aborts with a helpful compile error if the lambda isn't a single field select or if the field
    * doesn't exist on the case class.
    */
  private def fieldInfoFromAccessor[T: c.WeakTypeTag, A](
      c: blackbox.Context
  )(accessor: c.Expr[T => A]): (String, Int) = {
    import c.universe._

    def fail(reason: String): Nothing =
      c.abort(
        accessor.tree.pos,
        s"Expected a simple field accessor like `_.fieldName`, got: $reason"
      )

    def extract(tree: Tree): String = tree match {
      case Function(_, Select(_, name))           => name.decodedName.toString
      case Function(_, Typed(Select(_, name), _)) => name.decodedName.toString
      case Function(_, Block(_, Select(_, name))) => name.decodedName.toString
      case Block(_, inner)                        => extract(inner)
      case other                                  => fail(showCode(other))
    }

    val name = extract(accessor.tree)

    // Walk the *primary constructor*'s parameter list rather than `tType.decls`. `decls` is unordered in Scala 2
    // reflection and would mis-index the synthetic `apply$default$<n>` lookup for case classes with > 1 default field.
    val tType        = c.weakTypeOf[T]
    val primaryCtor  = tType.decl(termNames.CONSTRUCTOR) match {
      case ms: MethodSymbol if ms.isPrimaryConstructor => ms
      case _                                           =>
        tType.decls
          .collectFirst { case m: MethodSymbol if m.isPrimaryConstructor => m }
          .getOrElse(c.abort(accessor.tree.pos, s"Could not find primary constructor of $tType."))
    }
    val ctorParams = primaryCtor.paramLists.headOption.getOrElse(Nil).map(_.name.decodedName.toString)
    val idx        = ctorParams.indexOf(name)

    if (idx < 0) {
      c.abort(
        accessor.tree.pos,
        s"Field '$name' does not exist on $tType. Available fields: [${ctorParams.mkString(", ")}]"
      )
    }

    (name, idx)
  }

}
