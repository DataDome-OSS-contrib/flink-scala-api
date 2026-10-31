package org.apache.flinkx.api.evolution

/** Evolving companion: the companion object of a versioned ADT, which holds the evolutions of the ADT.
  * {{{
  * @version(2)
  * @renamed(since = 2, "FormerOrder")
  * @postEvolution(Order.fix)
  * case class Order(id: String, @added(since = 2) note: String = "none")
  *
  * object Order extends Evolving[Order] {
  *   val evolutions = Evolutions[Order]
  *
  *   private[example] def fix(formerVersion: Int, order: Order): Order = order // Visible where Order is declared
  * }
  * }}}
  *
  * Each versioned ADT needs an evolving companion: the derivation of its type information checks it at compile time. A
  * case object has no companion: its sealed trait declares its evolutions.
  *
  * [[Evolutions.apply]] reads the annotations at compile time. At the first lookup, the registry reads the evolutions
  * of all the evolving companions in the class loader of the job.
  */
trait Evolving[T] {

  /** The evolutions of the ADT: `Evolutions[T]` builds them from its annotations. */
  def evolutions: Evolutions[T]

}
