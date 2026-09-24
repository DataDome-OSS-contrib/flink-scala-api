package org.apache.flinkx.api.evolution

import scala.quoted.*

/** Evidence that the evolutions of `T` can be declared, required to derive its type information.
  *
  * Materialized by a macro wherever the derivation of `T` is expanded: a versioned ADT whose companion doesn't extend
  * [[Evolved]] fails the build, rather than restoring without its evolutions.
  */
final class Evolvable[T] private[evolution] ()

object Evolvable:

  private val instance = new Evolvable[Any]()

  inline given derived[T]: Evolvable[T] = ${ derivedImpl[T] }

  private def derivedImpl[T: Type](using Quotes): Expr[Evolvable[T]] =
    import quotes.reflect.*

    val tpe    = TypeRepr.of[T]
    val symbol = tpe.typeSymbol

    // A version belongs to an ADT, and scalac does not enforce the target of an annotation
    val parameters = if symbol.primaryConstructor.isNoSymbol then Nil else symbol.primaryConstructor.paramSymss.flatten
    parameters
      .find(Declare.isVersioned)
      .foreach(parameter => report.errorAndAbort(evolutionNotAllowed("version", s"$symbol.${parameter.name}")))

    // A version 0 declares nothing to evolve from, and a case object is declared by the sealed trait it belongs to
    if Declare.versionOf(symbol) > 0 && !symbol.flags.is(Flags.Module) then
      // An enum value is typed by its enum, which declares for it
      val companion = Option.when(symbol.companionModule.exists)(symbol.companionModule.termRef)
      val evolved   = Symbol.requiredClass(classOf[Evolved[?]].getName).typeRef.appliedTo(symbol.typeRef)
      if !companion.exists(_ <:< evolved) then report.errorAndAbort(companionNotEvolved(symbol.fullName))
    '{ Evolvable.instance.asInstanceOf[Evolvable[T]] }
