package org.apache.flinkx.api.evolution

/** The givens of [[Evolvable]], which differ between Scala 2 and Scala 3. */
private[evolution] trait EvolvableDerivation:

  inline given derived[T]: Evolvable[T] = ${ EvolutionMacro.evolvableImpl[T] }
