package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.{evolutionNotAllowed => notAllowed}

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/** Declares the evolutions of an ADT from its annotations alone.
  *
  * Reads what the derivation used to read, but emits only the registration code: no field typeclass is summoned, so it
  * compiles wherever the ADT is visible, and it can be generated for every annotated ADT of a module.
  */
object Declare {

  def declare[T]: Unit = macro declareImpl[T]

  def declareImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Unit] = {
    import c.universe._

    val evolution = q"_root_.org.apache.flinkx.api.evolution"
    val tpe       = weakTypeOf[T]
    val symbol    = tpe.typeSymbol
    val children  = if (symbol.isClass) symbol.asClass.knownDirectSubclasses.toList.sortBy(_.fullName) else Nil

    val parameters = tpe.decls
      .collectFirst { case method: MethodSymbol if method.isPrimaryConstructor => method }
      .toList
      .flatMap(_.paramLists.flatten)

    /** Members of the ADT, in declaration order: field names or subtype class names. */
    val memberNames: Tree =
      if (children.isEmpty) q"_root_.scala.Array(..${parameters.map(p => q"${p.name.decodedName.toString}")})"
      else q"_root_.scala.Array(..${children.map(child => q"_root_.scala.reflect.classTag[${child.asType.toType}].runtimeClass.getName")})"

    // Compared by symbol rather than by type: the generic annotations have no type tag to compare against
    def annotationSymbol(name: String): Symbol = c.mirror.staticClass(s"org.apache.flinkx.api.$name")
    val Renamed                                = annotationSymbol("renamed")
    val DeletedFields                          = annotationSymbol("deletedFields")
    val DeletedClasses                         = annotationSymbol("deletedClasses")
    val PostDeserialize                        = annotationSymbol("postDeserialize")
    val Added                                  = annotationSymbol("added")
    val Transformed                            = annotationSymbol("transformed")

    /** Schema version declared by the `version` annotation, read from the tree: it is a Java annotation, which the
      * macro cannot instantiate, but whose argument is a constant.
      */
    def versionOf(owner: Symbol): Int =
      owner.annotations
        .find(_.tree.tpe.typeSymbol.fullName == "org.apache.flinkx.api.version")
        .flatMap(_.tree match {
          case Apply(_, args) =>
            args.collectFirst {
              case Literal(Constant(declared: Int))              => declared
              case NamedArg(_, Literal(Constant(declared: Int))) => declared
            }
          case _ => None
        })
        .getOrElse(0)

    def is(annotation: Annotation, annotationType: Symbol): Boolean =
      annotation.tree.tpe.typeSymbol == annotationType

    def isVersioned(owner: Symbol): Boolean =
      owner.annotations.exists(_.tree.tpe.typeSymbol.fullName == "org.apache.flinkx.api.version")

    /** The annotation as an expression to splice, detyped: it was typed in the context of the class declaring it. */
    def instanceOf(annotation: Annotation): Tree = c.untypecheck(annotation.tree)

    def classAnnotations(builder: TermName, clazz: TermName): List[Tree] = symbol.annotations.collect {
      case a if is(a, Renamed) =>
        q"""val r = ${instanceOf(a)}; $builder.registerFormerClass(r.formerName, $clazz, r.since)"""
      case a if is(a, DeletedFields) =>
        q"""val d = ${instanceOf(a)}
            d.formerNames.foreach(name => $builder.fieldEvolutions += $evolution.FieldEvolution.Delete(d.since, $clazz, name))"""
      case a if is(a, DeletedClasses) =>
        q"""val d = ${instanceOf(a)}
            d.formerClassNames.foreach(name => $builder.registerDeletedFormerClass(name, $clazz, d.since, d.throwOnInstance))"""
      case a if is(a, PostDeserialize) =>
        q"""$builder.addPostDeserialize(${instanceOf(
            a
          )}.asInstanceOf[_root_.org.apache.flinkx.api.postDeserialize[$tpe]])"""
    }

    def fieldAnnotations(builder: TermName, clazz: TermName): List[Tree] =
      parameters.zipWithIndex.flatMap { case (parameter, index) =>
        val label = parameter.name.decodedName.toString
        parameter.annotations.collect {
          case a if is(a, Added) =>
            q"""$builder.fieldEvolutions += $evolution.FieldEvolution.Add(${instanceOf(
                a
              )}.since, $clazz, $label, $evolution.Evolutions.defaultFieldValue($clazz, $index))"""
          case a if is(a, Renamed) =>
            q"""val r = ${instanceOf(a)}
                $builder.fieldEvolutions += $evolution.FieldEvolution.Rename(r.since, $clazz, r.formerName, $label)"""
          case a if is(a, Transformed) =>
            q"""val t = ${instanceOf(a)}
                $builder.fieldEvolutions += $evolution.FieldEvolution.Transform(t.since, $clazz, $label, t.mapper)"""
        }
      }

    /** Reject the evolution annotations that declare nothing where they are, before anything is generated. */
    def validate(): Unit = {
      val version              = versionOf(symbol)
      val hasVersionedAncestor = tpe.baseClasses.filterNot(_ == symbol).exists(ancestor => isVersioned(ancestor))
      val isCoproduct          = children.nonEmpty

      def reject(annotation: Annotation, target: String): Nothing =
        c.abort(
          c.enclosingPosition,
          notAllowed(annotation.tree.tpe.typeSymbol.name.decodedName.toString, target)
        )

      def isEvolved(annotation: Annotation): Boolean =
        annotation.tree.tpe <:< typeOf[org.apache.flinkx.api.Evolved]

      symbol.annotations.filter(isEvolved).foreach { annotation =>
        val allowed =
          if (version == 0) is(annotation, Renamed) && hasVersionedAncestor
          else
            is(annotation, Renamed) || is(annotation, DeletedClasses) || is(annotation, PostDeserialize) ||
            (is(annotation, DeletedFields) && !isCoproduct)
        if (!allowed) {
          reject(annotation, if (version == 0) s"${symbol.fullName} with version 0" else symbol.fullName)
        }
      }

      if (symbol.annotations.count(annotation => is(annotation, PostDeserialize)) > 1) {
        c.abort(c.enclosingPosition, notAllowed("postDeserialize", s"${symbol.fullName} twice"))
      }

      parameters.foreach { parameter =>
        parameter.annotations.filter(isEvolved).foreach { annotation =>
          val allowed =
            version > 0 && (is(annotation, Added) || is(annotation, Renamed) || is(annotation, Transformed))
          val suffix = if (version == 0) " with version 0" else ""
          if (!allowed) reject(annotation, s"${symbol.fullName}.${parameter.name.decodedName}$suffix")
        }
      }

      // A subtype declares its own evolutions, which its own declaration registers
      children.filterNot(isVersioned).foreach { child =>
        child.annotations.filter(isEvolved).foreach(annotation => reject(annotation, child.fullName))
      }
    }

    validate()
    val builder = TermName("builder")
    val clazz   = TermName("declaredClass")
    c.Expr[Unit](q"""
      val $clazz   = _root_.scala.reflect.classTag[$tpe].runtimeClass.asInstanceOf[_root_.java.lang.Class[$tpe]]
      val $builder = new $evolution.EvolutionBuilder[$tpe]($clazz, $evolution.Evolutions.findVersion($clazz), $memberNames)
      ..${classAnnotations(builder, clazz)}
      ..${fieldAnnotations(builder, clazz)}
      $evolution.Evolutions.register($builder)
    """)
  }

}
