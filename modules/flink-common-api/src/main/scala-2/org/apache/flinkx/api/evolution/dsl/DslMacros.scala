package org.apache.flinkx.api.evolution.dsl

import scala.reflect.macros.blackbox

/** Scala 2 blackbox macros backing the typed-accessor overloads of the evolution DSL (e.g. `.added(_.fieldInFile)`).
  *
  * Each macro pattern-matches the AST of the supplied `_.field` lambda to extract the selected field name and rewrites
  * the call site into the corresponding string-based DSL method on [[EvolutionDslAt]]. A compile error is emitted if
  * the lambda is anything more elaborate than a single field select, or if the selected field does not exist on the
  * case class.
  */
object DslMacros {

  def added[T: c.WeakTypeTag, A](
      c: blackbox.Context
  )(accessor: c.Expr[T => A]): c.Expr[EvolutionDslAt[T]] = {
    import c.universe._
    val name = fieldNameFromAccessor(c)(accessor)
    // `c.prefix.tree` is the implicit-class wrapper `EvolutionDslAccessors(dsl)`; the underlying builder lives in
    // its `dsl` field.
    c.Expr[EvolutionDslAt[T]](q"${c.prefix.tree}.dsl.added($name)")
  }

  def renamed[T: c.WeakTypeTag, A](
      c: blackbox.Context
  )(accessor: c.Expr[T => A], formerName: c.Expr[String]): c.Expr[EvolutionDslAt[T]] = {
    import c.universe._
    val name = fieldNameFromAccessor(c)(accessor)
    c.Expr[EvolutionDslAt[T]](q"${c.prefix.tree}.dsl.renamed($formerName, $name)")
  }

  def transformed[T: c.WeakTypeTag, A, B](
      c: blackbox.Context
  )(accessor: c.Expr[T => B], mapper: c.Expr[A => B]): c.Expr[EvolutionDslAt[T]] = {
    import c.universe._
    val name = fieldNameFromAccessor(c)(accessor)
    c.Expr[EvolutionDslAt[T]](q"${c.prefix.tree}.dsl.transformed($name, $mapper)")
  }

  /** Pattern-match the lambda's AST to extract the selected field name. Recognises `_.field`, `t => t.field`, and the
    * variants surrounded by `Typed`/`Block` wrappers the compiler may emit. Anything else aborts compilation.
    */
  private def fieldNameFromAccessor[T: c.WeakTypeTag, A](
      c: blackbox.Context
  )(accessor: c.Expr[T => A]): c.Expr[String] = {
    import c.universe._

    def fail(reason: String): Nothing =
      c.abort(
        accessor.tree.pos,
        s"Expected a simple field accessor like `_.fieldName`, got: $reason"
      )

    def extract(tree: Tree): String = tree match {
      case Function(_, Select(_, name))                          => name.decodedName.toString
      case Function(_, Typed(Select(_, name), _))                => name.decodedName.toString
      case Function(_, Block(_, Select(_, name)))                => name.decodedName.toString
      case Block(_, inner)                                       => extract(inner)
      case other                                                 => fail(showCode(other))
    }

    val name = extract(accessor.tree)

    // Verify the field exists on T at macro time.
    val tType   = c.weakTypeOf[T]
    val members = tType.decls.collect {
      case s if s.isTerm && s.asTerm.isCaseAccessor => s.name.decodedName.toString
    }
    if (members.nonEmpty && !members.exists(_ == name)) {
      c.abort(
        accessor.tree.pos,
        s"Field '$name' does not exist on $tType. Available fields: [${members.mkString(", ")}]"
      )
    }

    c.Expr[String](Literal(Constant(name)))
  }

}
