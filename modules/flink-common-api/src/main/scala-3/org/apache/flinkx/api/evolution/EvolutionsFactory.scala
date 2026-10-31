package org.apache.flinkx.api.evolution

/** The macro of [[Evolutions]], which differs between Scala 2 and Scala 3. */
private[evolution] trait EvolutionsFactory:

  /** Build the evolutions of `T` from its annotations, in the evolving companion of `T`. A misplaced annotation stops
    * the compilation.
    */
  inline def apply[T]: Evolutions[T] = ${ EvolutionMacro.evolutionsImpl[T] }
