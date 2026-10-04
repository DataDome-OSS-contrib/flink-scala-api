package org.apache.flinkx.api.evolution

import scala.language.experimental.macros

/** The macro of [[Evolutions]], which differs between Scala 2 and Scala 3. */
private[evolution] trait EvolutionsFactory {

  /** Build the evolutions of `T` from its annotations, from the companion of `T` so that a mapper private to the
    * companion is visible. Rejects a misplaced annotation at compile time.
    */
  def apply[T]: Evolutions[T] = macro EvolutionMacro.evolutionsImpl[T]

}
