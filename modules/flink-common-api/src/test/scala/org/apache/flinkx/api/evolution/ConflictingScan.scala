package org.apache.flinkx.api.evolution

/** Gives the declarations of the fixtures in conflict to the companion scan, only when a test switches them on. */
object ConflictingScan {

  @volatile private var switchedOn: Option[String] = None

  /** The evolutions of a fixture in conflict, or no evolution when the test does not switch this fixture on. */
  def evolutionsOf[T](clazz: Class[T], fixture: String, declaring: => Evolutions[T]): Evolutions[T] =
    if (switchedOn.contains(fixture)) declaring else new Evolutions(clazz, () => Nil)

  /** Runs `body` with a registry that contains the given fixture in conflict, then empties the registry. */
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
