package org.apache.flinkx.api.evolution

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/** Evidence that the evolutions of `T` can be declared, required to derive its type information.
  *
  * Materialized by a macro wherever the derivation of `T` is expanded: a versioned ADT whose companion doesn't extend
  * [[Evolved]] fails the build, rather than restoring without its evolutions.
  */
final class Evolvable[T] private[evolution] ()

object Evolvable extends LowPriorityEvolvable {

  // Public, as the macro splices it in the code of the derivation site
  val instance = new Evolvable[Any]()

  // The tuples this library derives itself declare nothing: a macro of this module cannot expand within it anyway
  implicit def tuple2[A, B]: Evolvable[(A, B)]             = instance.asInstanceOf[Evolvable[(A, B)]]
  implicit def tuple3[A, B, C]: Evolvable[(A, B, C)]       = instance.asInstanceOf[Evolvable[(A, B, C)]]
  implicit def tuple4[A, B, C, D]: Evolvable[(A, B, C, D)] = instance.asInstanceOf[Evolvable[(A, B, C, D)]]

}

private[evolution] trait LowPriorityEvolvable {

  implicit def derived[T]: Evolvable[T] = macro Evolvable.derivedImpl[T]

  def derivedImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Evolvable[T]] = {
    import c.universe._

    val tpe    = weakTypeOf[T]
    val symbol = tpe.typeSymbol

    // A version belongs to an ADT, and scalac does not enforce the target of an annotation
    tpe.decls
      .collectFirst { case method: MethodSymbol if method.isPrimaryConstructor => method }
      .toList
      .flatMap(_.paramLists.flatten)
      .find(parameter => Declare.isVersioned(c)(parameter))
      .foreach(parameter =>
        c.abort(c.enclosingPosition, evolutionNotAllowed("version", s"$symbol.${parameter.name.decodedName.toString}"))
      )

    // A version 0 declares nothing to evolve from, and a case object is declared by the sealed trait it belongs to
    if (Declare.versionOf(c)(symbol) > 0 && !symbol.isModuleClass) {
      val companion = symbol.companion
      val evolved   = appliedType(typeOf[Evolved[_]].typeConstructor, tpe)
      if (companion == NoSymbol || !(companion.typeSignature <:< evolved)) {
        c.abort(c.enclosingPosition, companionNotEvolved(symbol.fullName))
      }
    }
    c.Expr[Evolvable[T]](
      q"_root_.org.apache.flinkx.api.evolution.Evolvable.instance.asInstanceOf[_root_.org.apache.flinkx.api.evolution.Evolvable[$tpe]]"
    )
  }

}
