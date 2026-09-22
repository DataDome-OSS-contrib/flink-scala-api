package org.apache.flinkx.api.evolution.pkg

import org.apache.flinkx.api.{deletedFields, version}

/** Versioned ADT nested in an object, as most of them are declared. */
object Nested {

  @version(1)
  @deletedFields(since = 1, "gone")
  case class Customer(name: String)

  case class NotVersioned(name: String)
}
