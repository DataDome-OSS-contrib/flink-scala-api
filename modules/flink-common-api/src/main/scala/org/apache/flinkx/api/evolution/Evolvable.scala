package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal

/** Evidence that the evolutions of `T` can be declared, required to derive its type information.
  *
  * Materialized by a macro wherever the derivation of `T` is expanded: a versioned ADT whose companion doesn't extend
  * [[Evolving]] fails the build, rather than restoring without its evolutions.
  *
  * @param version
  *   Current schema version of `T`, read from its `@version` at compile time. The one of its enum for an enum value
  */
@Internal
final class Evolvable[T](val version: Int)

object Evolvable extends EvolvableDerivation {

  /** Evidence for a type that declares no version. */
  val unversioned: Evolvable[Any] = new Evolvable[Any](0)

}
