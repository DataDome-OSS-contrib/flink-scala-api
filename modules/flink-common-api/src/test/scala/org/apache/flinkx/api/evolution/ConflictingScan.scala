package org.apache.flinkx.api.evolution

/** Hands the scan of the jars the declarations of the fixtures conflicting on purpose, only while switched on. */
object ConflictingScan {

  @volatile private var switchedOn: Option[String] = None

  /** The evolutions the companion of a conflicting fixture declares, nothing until the given fixture is switched on. */
  def evolutionsOf[T](clazz: Class[T], fixture: String, declaring: => Evolutions[T]): Evolutions[T] =
    if (switchedOn.contains(fixture)) declaring else new Evolutions(clazz, () => Nil)

  /** Runs `body` with a registry declaring the given conflicting fixture, then empties it again. */
  def withFixture[A](fixture: String)(body: => A): A = {
    Evolutions.reset()
    switchedOn = Some(fixture)
    try body
    finally {
      switchedOn = None
      Evolutions.reset()
    }
  }

}
