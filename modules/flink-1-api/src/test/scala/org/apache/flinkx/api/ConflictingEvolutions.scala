package org.apache.flinkx.api

import org.apache.flinkx.api.ConflictingEvolutions.{FirstClaiming, SecondClaiming}
import org.apache.flinkx.api.evolution.{Evolutions, Evolving, renamed, version}

/** Hands the scan of the jars two declarations claiming the same former name, only while switched on. */
object ConflictingEvolutions {

  @volatile var on = false

  def evolutionsOf[T](clazz: Class[T], claiming: => Evolutions[T]): Evolutions[T] =
    if (on) claiming else new Evolutions(clazz, () => Nil)

  // Not found by the scan, which only reads the direct parents of a companion
  trait Unscanned[T] extends Evolving[T]

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class FirstClaiming(a: String)
  object FirstClaiming extends Unscanned[FirstClaiming] {
    lazy val evolutions = Evolutions[FirstClaiming]
  }

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class SecondClaiming(a: String)
  object SecondClaiming extends Unscanned[SecondClaiming] {
    lazy val evolutions = Evolutions[SecondClaiming]
  }

}

object FirstClaimingCompanion extends Evolving[FirstClaiming] {
  def evolutions: Evolutions[FirstClaiming] =
    ConflictingEvolutions.evolutionsOf(classOf[FirstClaiming], FirstClaiming.evolutions)
}

object SecondClaimingCompanion extends Evolving[SecondClaiming] {
  def evolutions: Evolutions[SecondClaiming] =
    ConflictingEvolutions.evolutionsOf(classOf[SecondClaiming], SecondClaiming.evolutions)
}
