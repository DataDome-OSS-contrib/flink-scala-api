package org.apache.flinkx.api.evolution

/** Hands the scan of the jars the declarations of the fixtures claiming the same former name, only when switched on. */
object ConflictingScan {

  @volatile var on = false

  def evolutionsOf[T](clazz: Class[T], claiming: => Evolutions[T]): Evolutions[T] =
    if (on) claiming else new Evolutions(clazz, () => Nil)

}
