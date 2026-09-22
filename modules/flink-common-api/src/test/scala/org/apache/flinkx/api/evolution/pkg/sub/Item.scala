package org.apache.flinkx.api.evolution.pkg.sub

import org.apache.flinkx.api.{renamed, version}

/** Versioned ADT of a sub-package of the scanned one. */
@version(1)
@renamed(since = 1, "Article")
case class Item(sku: String)
