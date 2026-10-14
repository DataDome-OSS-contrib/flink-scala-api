package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal

/** Evidence that the evolutions of `T` can be declared, required to derive its type information.
  *
  * Materialized by a macro wherever the derivation of `T` is expanded: a versioned ADT whose companion doesn't extend
  * [[Evolving]] fails the build, rather than restoring without its evolutions, and so does a companion declaring them
  * in a strict `val`, which the derivation may read before it is initialized.
  *
  * @param version
  *   Current schema version of `T`, read from its `@version` at compile time. The one of its enum for an enum value
  * @param declaring
  *   The evolutions declared by the companion of `T`, or of the sealed trait or enum declaring it for a case object or
  *   an enum value, [[Evolutions.none]] if no such companion extends [[Evolving]]
  */
@Internal
final class Evolvable[T](val version: Int, declaring: () => Evolutions[_]) {

  /** The evolution of `clazz`, the runtime class of `T`, at its current version, read from the evolutions declaring it.
    */
  def evolution[A](clazz: Class[A]): Evolution[A] = declaring().evolution(clazz, version)

}

object Evolvable extends EvolvableDerivation {

  /** Evidence for a type that declares no version. */
  val unversioned: Evolvable[Any] = new Evolvable[Any](0, () => Evolutions.none)

}
