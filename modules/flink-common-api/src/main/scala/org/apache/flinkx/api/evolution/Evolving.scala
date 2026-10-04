package org.apache.flinkx.api.evolution

/** Declares the evolutions of a versioned ADT, from its companion:
  * {{{
  * @version(2)
  * @renamed(since = 2, "FormerOrder")
  * @postEvolution(Order.fix)
  * case class Order(id: String, @added(since = 2) note: String = "none")
  *
  * object Order extends Evolving[Order] {
  *   val evolutions = Evolutions[Order]
  *
  *   private[this] def fix(version: Int, order: Order): Order = order // Only the annotations need to see the mapper
  * }
  * }}}
  *
  * Every ADT annotated with `@version` needs this on its companion, which the derivation of its type information checks
  * at compile time. A case object has no companion of its own: the sealed trait it belongs to declares it.
  *
  * The annotations are read at compile time by [[Evolutions.apply]], and the evolutions are handed over to the registry
  * when the companion initializes: a snapshot naming the ADT initializes its companion to get them, so they are
  * available wherever the state descriptor was built, on the TaskManager restoring the state as on the client deriving
  * the type information.
  */
trait Evolving[T] {

  /** The evolutions of the ADT, built from its annotations by `Evolutions[T]`. */
  def evolutions: Evolutions[T]

}
