package org.apache.flinkx.api

import scala.quoted.*

/** Reads annotation trees whole, which a macro must do before splicing any of them.
  *
  * Splicing an annotation that carries a function makes a later splice of that same annotation read a stale tree, which
  * crashes the expansion (`MatchError` in `TreeUnpickler`, Scala 3.3) as soon as the ADT comes from an already compiled
  * source, as in any incremental build.
  */
private[api] object AnnotationTrees {

  /** Reads whole the annotations of the given symbols, leaving nothing for the splices to read back. */
  private[api] def readWhole(using q: Quotes)(owners: List[q.reflect.Symbol]): Unit = {
    import q.reflect.*
    object Reader extends TreeTraverser
    owners.foreach(owner => owner.annotations.foreach(Reader.traverseTree(_)(owner)))
  }

  /** Reads whole the annotations `A` declares, inherits, or carries on its fields. */
  private[api] inline def readWholeOf[A]: Unit = ${ readWholeOfImpl[A] }

  private def readWholeOfImpl[A: Type](using q: Quotes): Expr[Unit] = {
    import q.reflect.*
    val tpe    = TypeRepr.of[A]
    val owners = (tpe.typeSymbol :: tpe.baseClasses).flatMap { clazz =>
      val constructor = clazz.primaryConstructor
      // A trait declares no constructor, so it carries no annotated parameter
      val parameters = if (constructor.isNoSymbol) Nil else constructor.paramSymss.flatten
      clazz :: parameters ::: clazz.declaredFields
    }
    readWhole(owners)
    '{ () }
  }

}
