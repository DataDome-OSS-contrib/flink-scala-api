package org.apache.flinkx.api.evolution

/** The macro of [[Evolutions]], which differs between Scala 2 and Scala 3. */
private[evolution] trait EvolutionsFactory:

  /** Build the evolutions of `T` from its annotations, from the companion of `T` so that a mapper private to the
    * companion is visible. Rejects a misplaced annotation at compile time.
    */
  inline def apply[T]: Evolutions[T] = ${ EvolutionMacro.evolutionsImpl[T] }
