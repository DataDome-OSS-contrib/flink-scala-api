package org.apache.flinkx.api.evolution.dsl

/** Scala 3 typed-accessor extension methods for [[EvolutionDslAt]]. Each method takes a `_.field` lambda, extracts the
  * field name at compile time via [[DslMacros.fieldName]], and delegates to the string-based DSL method.
  *
  * The names are intentionally distinct from the string-based methods (`addedField` vs `added`) to keep cross-platform
  * source compatibility with Scala 2, where overloading a class method with an implicit-class method by argument shape
  * leaves the compiler unable to infer parameter types for `_.field` lambdas.
  *
  * Example:
  * {{{
  *   Evolution.of[Click]
  *     .version(3).addedField(_.fieldInFile)
  *     .version(2).renamedField(_.id, formerName = "identifier")
  *     .build
  * }}}
  */
extension [T](dsl: EvolutionDslAt[T])

  /** Typed-accessor equivalent of `.added(name)`. */
  inline def addedField[A](inline accessor: T => A): EvolutionDslAt[T] =
    dsl.added(DslMacros.fieldName[T, A](accessor))

  /** Typed-accessor equivalent of `.renamed(formerName, currentName)`. */
  inline def renamedField[A](inline accessor: T => A, formerName: String): EvolutionDslAt[T] =
    dsl.renamed(formerName, DslMacros.fieldName[T, A](accessor))

  /** Typed-accessor equivalent of `.transformed[A, B](name, mapper)`. */
  inline def transformedField[A, B](inline accessor: T => B, mapper: A => B): EvolutionDslAt[T] =
    dsl.transformed[A, B](DslMacros.fieldName[T, B](accessor), mapper)
