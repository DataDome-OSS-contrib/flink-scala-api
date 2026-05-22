package org.apache.flinkx.api.evolution.dsl

import scala.language.experimental.macros

/** Scala 2 typed-accessor overloads for [[EvolutionDslAt]]. Provided as an implicit conversion, each method takes a
  * `_.field` lambda, extracts the field name at macro-expansion time via [[DslMacros]], and rewrites the call into the
  * corresponding string-based DSL method.
  *
  * Example:
  * {{{
  *   import org.apache.flinkx.api.evolution.dsl._
  *   Evolution.of[Click]
  *     .version(3).added(_.fieldInFile)
  *     .version(2).renamed(formerName = "identifier", _.id)
  * }}}
  */
trait EvolutionDslAccessors[T] {

  /** Typed-accessor equivalent of `.added(name)`. Field name is extracted from `accessor` at macro-expansion time and
    * validated against the case class. */
  final def added[A](accessor: T => A): EvolutionDslAt[T] =
    macro DslMacros.added[T, A]

  /** Typed-accessor equivalent of `.renamed(formerName, currentName)`. Current name comes from `accessor`. */
  final def renamed[A](formerName: String, accessor: T => A): EvolutionDslAt[T] =
    macro DslMacros.renamed[T, A]

  /** Typed-accessor equivalent of `.transformed[A, B](name, mapper)`. Field name comes from `accessor`. */
  final def transformed[A, B](accessor: T => B, mapper: A => B): EvolutionDslAt[T] =
    macro DslMacros.transformed[T, A, B]

}
