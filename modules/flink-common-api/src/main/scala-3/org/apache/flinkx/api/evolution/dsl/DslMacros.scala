package org.apache.flinkx.api.evolution.dsl

import scala.quoted.*

/** Compile-time helpers that back the typed-accessor overloads of the evolution DSL (e.g. `.added(_.fieldInFile)`).
  *
  * The single primitive provided here, [[fieldNameMacro]], extracts the selected member name from a `_.field` lambda
  * and (when the type is a Magnolia-derivable case class) statically verifies that the named field exists on the
  * case class. It does *not* attempt to validate the field's type at any historical version — see the design notes in
  * `Evolved` for why intermediate-version type checking is out of scope.
  */
object DslMacros {

  /** Public entry point: returns the field name (as a `String`) selected by `accessor`. Use the inline wrapper below
    * to keep call sites tidy. */
  inline def fieldName[T, A](inline accessor: T => A): String =
    ${ fieldNameMacro[T, A]('accessor) }

  /** Quoted-macro implementation. Patterns we recognise:
    *   - `(t: T) => t.field` after `Inlined` wrappers
    *   - `_.field` — desugared by the compiler to the same shape with a synthetic parameter name
    *
    * Anything else (multi-step selects, method calls, blocks with side effects) is rejected at compile time with a
    * helpful error message pointing at the call site.
    */
  private def fieldNameMacro[T: Type, A: Type](accessor: Expr[T => A])(using Quotes): Expr[String] = {
    import quotes.reflect.*

    def fail(reason: String): Nothing =
      report.errorAndAbort(
        s"Expected a simple field accessor like `_.fieldName`, got: $reason",
        accessor.asTerm.pos
      )

    // Recursively unwrap Inlined/Block wrappers to reach the underlying lambda.
    def unwrap(term: Term): Term = term match {
      case Inlined(_, _, inner) => unwrap(inner)
      case Block(Nil, expr)     => unwrap(expr)
      case other                => other
    }

    val name: String = unwrap(accessor.asTerm) match {
      // The two common shapes the compiler produces for `_.field` and `(t: T) => t.field`.
      case Lambda(_, Select(_, fieldName))                                  => fieldName
      case Block(List(DefDef(_, _, _, Some(Select(_, fieldName)))), _)      => fieldName
      case other                                                            => fail(other.show(using Printer.TreeShortCode))
    }

    // Verify the field actually exists on T. Magnolia's later derivation also catches this, but failing here gives a
    // much better error pointing at the migration site rather than at an opaque derivation stack.
    val tType   = TypeRepr.of[T]
    val members = tType.typeSymbol.caseFields.map(_.name)
    if (members.nonEmpty && !members.contains(name)) {
      report.errorAndAbort(
        s"Field '$name' does not exist on ${tType.show}. Available fields: ${members.mkString("[", ", ", "]")}",
        accessor.asTerm.pos
      )
    }

    Expr(name)
  }

}
