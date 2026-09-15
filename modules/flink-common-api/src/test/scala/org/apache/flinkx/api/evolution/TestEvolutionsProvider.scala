package org.apache.flinkx.api.evolution

/** Listed in `META-INF/services`, so that a lookup missing `Probe` declares it without any derivation. */
class TestEvolutionsProvider extends EvolutionsProvider {
  override def declare(): Unit = Declare.declare[DeclareTest.Probe]
}
