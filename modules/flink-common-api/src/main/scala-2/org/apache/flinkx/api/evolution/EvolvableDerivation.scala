package org.apache.flinkx.api.evolution

import scala.language.experimental.macros

/** The implicits of [[Evolvable]], which differ between Scala 2 and Scala 3. */
private[evolution] trait EvolvableDerivation extends LowPriorityEvolvable {

  // The tuples this library derives itself declare nothing: a macro of this module cannot expand within it anyway
  implicit def tuple2[A, B]: Evolvable[(A, B)]             = Evolvable.instance.asInstanceOf[Evolvable[(A, B)]]
  implicit def tuple3[A, B, C]: Evolvable[(A, B, C)]       = Evolvable.instance.asInstanceOf[Evolvable[(A, B, C)]]
  implicit def tuple4[A, B, C, D]: Evolvable[(A, B, C, D)] = Evolvable.instance.asInstanceOf[Evolvable[(A, B, C, D)]]

}

private[evolution] trait LowPriorityEvolvable {

  implicit def derived[T]: Evolvable[T] = macro EvolutionMacro.evolvableImpl[T]

}
