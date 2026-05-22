package org.apache.flinkx.api.evolution.dsl

/** Scala 3 typed-accessor methods mixed into [[EvolutionDslAt]]. Each method takes a `_.field` lambda, extracts the
  * field name at compile time via [[DslMacros.fieldName]], and delegates to the string-based DSL method defined on
  * [[EvolutionDslAt]] itself (overload resolution picks the `String` variant since `fieldName` returns a `String`).
  *
  * Example:
  * {{{
  *   Evolution.of[Click]
  *     .version(3).added(_.fieldInFile)
  *     .version(2).renamed(_.id, formerName = "identifier")
  * }}}
  */
trait EvolutionDslAccessors[T]:
  self: EvolutionDslAt[T] =>

  /** Typed-accessor equivalent of `.added(name: String)`. */
  inline def added[A](inline accessor: T => A): EvolutionDslAt[T] =
    self.added(DslMacros.fieldName[T, A](accessor))

  /** Typed-accessor equivalent of `.renamed(formerName, currentName)`. */
  inline def renamed[A](formerName: String, inline accessor: T => A): EvolutionDslAt[T] =
    self.renamed(formerName, DslMacros.fieldName[T, A](accessor))

  /** Typed-accessor equivalent of `.transformed[A, B](name, mapper)`. */
  inline def transformed[A, B](inline accessor: T => B, mapper: A => B): EvolutionDslAt[T] =
    self.transformed[A, B](DslMacros.fieldName[T, B](accessor), mapper)
