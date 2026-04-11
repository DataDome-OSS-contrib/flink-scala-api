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
  *     .version(2).renamed(_.id, formerName = "identifier")
  *     .build
  * }}}
  */
final class EvolutionDslAccessors[T](val dsl: EvolutionDslAt[T]) extends AnyVal {

  /** Typed-accessor equivalent of `.added(name)`. Field name is extracted from `accessor` at macro-expansion time and
    * validated against the case class. */
  def addedField[A](accessor: T => A): EvolutionDslAt[T] =
    macro DslMacros.added[T, A]

  /** Typed-accessor equivalent of `.renamed(formerName, currentName)`. Current name comes from `accessor`. */
  def renamedField[A](accessor: T => A, formerName: String): EvolutionDslAt[T] =
    macro DslMacros.renamed[T, A]

  /** Typed-accessor equivalent of `.transformed[A, B](name, mapper)`. Field name comes from `accessor`. */
  def transformedField[A, B](accessor: T => B, mapper: A => B): EvolutionDslAt[T] =
    macro DslMacros.transformed[T, A, B]

}

object EvolutionDslAccessors {

  /** Implicit conversion that augments [[EvolutionDslAt]] with the typed-accessor methods. */
  implicit def toAccessors[T](dsl: EvolutionDslAt[T]): EvolutionDslAccessors[T] =
    new EvolutionDslAccessors[T](dsl)

}
