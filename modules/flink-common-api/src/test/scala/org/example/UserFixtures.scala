package org.example

import org.apache.flinkx.api.{added, deletedClasses, deletedFields, postDeserialize, renamed, transformed, version}

/** A user ADT using every evolution annotation, outside the package of the library. */
object UserFixtures {

  @version(2)
  @renamed(since = 1, "FormerOrder")
  @deletedFields(since = 1, "gone")
  @deletedClasses(since = 2, throwOnInstance = false, "GoneType")
  @postDeserialize(bump)
  case class Order(
      @renamed(since = 1, "formerId") id: String,
      @transformed(since = 1, intToLabel) label: String,
      @added(since = 2) note: String = "none"
  )

  def intToLabel(i: Int): String              = i.toString
  def bump(version: Int, order: Order): Order = order.copy(note = order.note + version)
}
