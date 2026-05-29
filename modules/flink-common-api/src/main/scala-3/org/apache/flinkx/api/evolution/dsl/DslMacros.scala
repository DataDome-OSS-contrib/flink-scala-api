package org.apache.flinkx.api.evolution.dsl

import scala.quoted.*

/** Compile-time helpers backing the typed-accessor overloads of the evolution DSL.
  *
  * Two entry points:
  *   - [[fieldName]] — used by `.renamed` and `.transformed`. Extracts the field name from a `_.field` lambda and
  *     verifies the field exists on the case class.
  *   - [[addedCall]] — used by `.added`. Same extraction and existence check as [[fieldName]], *plus* it looks up
  *     the case class's synthetic default value for that field and emits the full `dsl.added(name, Some(default))`
  *     call. Aborts compilation if the field has no default — what would otherwise be a runtime
  *     `AddedFieldWithoutDefaultException` becomes a compile error.
  */
object DslMacros {

  /** Field-name extractor used by `.renamed` / `.transformed`. */
  inline def fieldName[T, A](inline accessor: T => A): String =
    ${ fieldNameMacro[T, A]('accessor) }

  /** Backs `.added(_.field)`. Emits `dsl.added(name, Some(default))` if the case class field has a default value,
    * otherwise aborts compilation. */
  inline def addedCall[T, A](
      inline dsl: EvolutionDslAt[T],
      inline accessor: T => A
  ): EvolutionDslAt[T] =
    ${ addedCallMacro[T, A]('dsl, 'accessor) }

  // -- Macro implementations --------------------------------------------------------------------

  private def fieldNameMacro[T: Type, A: Type](accessor: Expr[T => A])(using Quotes): Expr[String] = {
    import quotes.reflect.*
    val name = extractFieldName[T, A](accessor)
    Expr(name)
  }

  private def addedCallMacro[T: Type, A: Type](
      dsl: Expr[EvolutionDslAt[T]],
      accessor: Expr[T => A]
  )(using Quotes): Expr[EvolutionDslAt[T]] = {
    import quotes.reflect.*

    val name = extractFieldName[T, A](accessor)
    val tSym = TypeRepr.of[T].typeSymbol

    // Index in the primary-constructor val-parameter list — this is the index Scala uses for the synthetic
    // `$lessinit$greater$default$<n>` naming. Filtering by `isValDef` matches Magnolia's behaviour for case
    // classes with no extra non-val params (the only shape this DSL targets).
    val valParams = tSym.primaryConstructor.paramSymss.flatten.filter(_.isValDef)
    val paramIdx  = valParams.indexWhere(_.name == name)
    if (paramIdx < 0) {
      report.errorAndAbort(
        s"Field '$name' is not a case-class parameter of ${tSym.fullName}; cannot read its default value.",
        accessor.asTerm.pos
      )
    }

    // Scala 3 emits the default initialiser as `<EnclosingClass>.$lessinit$greater$default$<idx+1>` on the companion
    // *class* (`Foo$`). Magnolia looks it up via `companionClass.declaredMethod` and invokes it through the
    // companion module value's term ref — same shape here.
    val defaultName    = s"$$lessinit$$greater$$default$$${paramIdx + 1}"
    val defaultMembers = tSym.companionClass.declaredMethod(defaultName)

    defaultMembers.headOption match {
      case Some(defaultMethod) =>
        val base    = Ident(tSym.companionModule.termRef).select(defaultMethod)
        val tParams = defaultMethod.paramSymss.headOption.filter(_.forall(_.isType))
        val call    = tParams match {
          case Some(tps) => TypeApply(base, tps.map(TypeTree.ref))
          case _         => base
        }
        val defaultExpr = call.asExprOf[Any]
        '{ ${ dsl }.added(${ Expr(name) }, Some($defaultExpr)) }

      case None =>
        report.errorAndAbort(
          s"Field '$name' marked as added must have a default value in ${tSym.fullName}.",
          accessor.asTerm.pos
        )
    }
  }

  /** Common field-name extraction + case-class existence check, shared by both public macros. Returns the string
    * literally; the caller decides what to do with it (lift to `Expr[String]`, look up the default, etc.). */
  private def extractFieldName[T: Type, A: Type](accessor: Expr[T => A])(using Quotes): String = {
    import quotes.reflect.*

    def fail(reason: String): Nothing =
      report.errorAndAbort(
        s"Expected a simple field accessor like `_.fieldName`, got: $reason",
        accessor.asTerm.pos
      )

    def unwrap(term: Term): Term = term match {
      case Inlined(_, _, inner) => unwrap(inner)
      case Block(Nil, expr)     => unwrap(expr)
      case other                => other
    }

    val name: String = unwrap(accessor.asTerm) match {
      case Lambda(_, Select(_, fieldName))                             => fieldName
      case Block(List(DefDef(_, _, _, Some(Select(_, fieldName)))), _) => fieldName
      case other                                                       => fail(other.show(using Printer.TreeShortCode))
    }

    val tType   = TypeRepr.of[T]
    val members = tType.typeSymbol.caseFields.map(_.name)
    if (members.nonEmpty && !members.contains(name)) {
      report.errorAndAbort(
        s"Field '$name' does not exist on ${tType.show}. Available fields: ${members.mkString("[", ", ", "]")}",
        accessor.asTerm.pos
      )
    }
    name
  }

}
