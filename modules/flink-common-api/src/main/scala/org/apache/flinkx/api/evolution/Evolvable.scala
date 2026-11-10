package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal

/** Evidence that the derivation of the type information of `T` requires. It gives the current schema version of `T`.
  *
  * A macro creates it where the derivation of `T` expands. A versioned ADT without an evolving companion stops the
  * compilation: a restore without its evolutions would give wrong data.
  *
  * @param version
  *   Current schema version of `T`, from its `@version`. For an enum value, the version of its enum.
  */
@Internal
final class Evolvable[T](val version: Int)

object Evolvable extends EvolvableDerivation {

  /** Evidence for a type without `@version`. */
  val unversioned: Evolvable[Any] = new Evolvable[Any](0)

}
