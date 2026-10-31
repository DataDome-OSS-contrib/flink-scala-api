package org.apache.flinkx.api

import scala.quoted.*

/** Reads all the annotation trees of a type, which a macro must do before it splices one of them.
  *
  * After the splice of an annotation that contains a function, a second splice of this annotation reads a stale tree.
  * The expansion then fails (`MatchError` in `TreeUnpickler`, Scala 3.3) if the ADT comes from compiled code, as in an
  * incremental build.
  */
private[api] object AnnotationTrees {

  /** Reads all the annotations of the given symbols, so that a splice has nothing to read again. */
  private[api] def readWhole(using q: Quotes)(owners: List[q.reflect.Symbol]): Unit = {
    import q.reflect.*
    object Reader extends TreeTraverser
    owners.foreach(owner => owner.annotations.foreach(Reader.traverseTree(_)(owner)))
  }

  /** Reads all the annotations that `A` declares or inherits, and the annotations of its fields. */
  private[api] inline def readWholeOf[A]: Unit = ${ readWholeOfImpl[A] }

  private def readWholeOfImpl[A: Type](using q: Quotes): Expr[Unit] = {
    import q.reflect.*
    val tpe    = TypeRepr.of[A]
    val owners = (tpe.typeSymbol :: tpe.baseClasses).flatMap { clazz =>
      val constructor = clazz.primaryConstructor
      // A trait has no constructor, so it has no annotated parameter
      val parameters = if (constructor.isNoSymbol) Nil else constructor.paramSymss.flatten
      clazz :: parameters ::: clazz.declaredFields
    }
    readWhole(owners)
    '{ () }
  }

}
