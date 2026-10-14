package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.EvolutionTest.{FirstClaimingFormerName, SecondClaimingFormerName}

/** Hands the scan of the jars two declarations claiming the same former name, only while switched on. */
object ConflictingScan {

  @volatile var on = false

  def evolutionsOf[T](clazz: Class[T], claiming: => Evolutions[T]): Evolutions[T] =
    if (on) claiming else new Evolutions(clazz, () => Nil)

}

object FirstClaimingCompanion extends Evolving[FirstClaimingFormerName] {
  def evolutions: Evolutions[FirstClaimingFormerName] =
    ConflictingScan.evolutionsOf(classOf[FirstClaimingFormerName], FirstClaimingFormerName.evolutions)
}

object SecondClaimingCompanion extends Evolving[SecondClaimingFormerName] {
  def evolutions: Evolutions[SecondClaimingFormerName] =
    ConflictingScan.evolutionsOf(classOf[SecondClaimingFormerName], SecondClaimingFormerName.evolutions)
}
