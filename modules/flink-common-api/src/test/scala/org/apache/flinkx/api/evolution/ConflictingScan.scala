package org.apache.flinkx.api.evolution

/** Hands the scan of the jars the declarations of the fixtures conflicting on purpose, only while switched on. */
object ConflictingScan {

  /** Switches on the fixtures claiming the same former name. */
  @volatile var on = false

  /** Switches on the fixtures of one case of a class bearing a former name its version can't tell apart. */
  @volatile var bearing: Option[String] = None

  def evolutionsOf[T](clazz: Class[T], claiming: => Evolutions[T]): Evolutions[T] =
    if (on) claiming else none(clazz)

  def bearingEvolutionsOf[T](clazz: Class[T], fixture: String, declaring: => Evolutions[T]): Evolutions[T] =
    if (bearing.contains(fixture)) declaring else none(clazz)

  private def none[T](clazz: Class[T]): Evolutions[T] = new Evolutions(clazz, () => Nil)

}
