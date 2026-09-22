package org.apache.flinkx.api.evolution

/** Listed in `META-INF/services`, so the restore finds the evolutions without anything declaring them by hand. */
class RestoreEvolutionsProvider extends EvolutionsProvider {
  override def declare(): Unit = Declare.declare[EvolutionRestoreFixtures.Order]
}
