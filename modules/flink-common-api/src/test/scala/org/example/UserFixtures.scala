package org.example

import org.apache.flinkx.api.evolution._

/** A user ADT using every evolution annotation, outside the package of the library. */
object UserFixtures {

  @version(2)
  @renamed(since = 1, "FormerOrder")
  @deletedFields(since = 1, "gone")
  @deletedClasses(since = 2, throwOnInstance = false, "GoneType")
  @postEvolution(bump)
  case class Order(
      @renamed(since = 1, "formerId") id: String,
      @transformed(since = 1, intToLabel) label: String,
      @added(since = 2) note: String = "none"
  )
  object Order extends Evolving[Order] { val evolutions = Evolutions[Order] }

  def intToLabel(i: Int): String              = i.toString
  def bump(version: Int, order: Order): Order = order.copy(note = order.note + version)

  /** Its mappers are private to the package, the narrowest scope the annotations of the ADT can see. */
  @version(1)
  @postEvolution(Invoice.stamp)
  case class Invoice(@transformed(since = 1, Invoice.toAmount) amount: String)
  object Invoice extends Evolving[Invoice] {
    val evolutions = Evolutions[Invoice]

    private[example] def toAmount(cents: Int): String                   = s"$cents cents"
    private[example] def stamp(version: Int, invoice: Invoice): Invoice = Invoice(s"${invoice.amount} since v$version")
  }
}
