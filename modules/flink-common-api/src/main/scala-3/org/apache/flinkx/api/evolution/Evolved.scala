package org.apache.flinkx.api.evolution

/** Declares the evolutions of a versioned ADT, from its companion:
  * {{{
  * @version(2)
  * @renamed(since = 2, "FormerOrder")
  * case class Order(id: String, @added(since = 2) note: String = "none")
  *
  * object Order extends Evolved[Order] {
  *   private def fix(version: Int, order: Order): Order = order // A mapper private to the companion is visible here
  * }
  * }}}
  *
  * Every ADT annotated with `@version` needs this on its companion, which the derivation of its type information checks
  * at compile time. A case object has no companion of its own: the sealed trait it belongs to declares it.
  *
  * The annotations are read at compile time, and their evolutions are handed over to [[Evolutions]] when the companion
  * initializes: a snapshot naming the ADT initializes its companion to get them, so they are available wherever the
  * state descriptor was built, on the TaskManager restoring the state as on the client deriving the type information.
  */
trait Evolved[T](using declaration: Declaration[T]):
  Evolutions.pending(declaration, getClass)
