package org.apache.flinkx.api.evolution.pkg

import org.apache.flinkx.api.{renamed, version}

/** Top-level versioned ADT of the scanned package. */
@version(1)
@renamed(since = 1, "Purchase")
case class Order(id: String)
