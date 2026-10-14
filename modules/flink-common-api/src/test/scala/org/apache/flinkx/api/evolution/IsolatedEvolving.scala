package org.apache.flinkx.api.evolution

/** Declares an ADT without being found by the scan of the jars, which only reads the direct parents of a companion.
  *
  * For the fixtures declaring evolutions wrongly, or claiming the former names of other fixtures on purpose: a test
  * registers them with [[Evolutions.declareInstead]], and never along with the well declared ones.
  */
trait IsolatedEvolving[T] extends Evolving[T]
