package org.apache.flinkx.api.evolution

/** Evidence that the evolutions of `T` can be declared, required to derive its type information.
  *
  * Materialized by a macro wherever the derivation of `T` is expanded: a versioned ADT whose companion doesn't extend
  * [[Evolving]] fails the build, rather than restoring without its evolutions.
  */
final class Evolvable[T] private[evolution] ()

object Evolvable extends EvolvableDerivation {

  // Public, as the macro splices it in the code of the derivation site
  val instance: Evolvable[Any] = new Evolvable[Any]()

}
