package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.{evolutionNotAllowed => notAllowed}

import scala.reflect.macros.blackbox
import scala.util.control.NonFatal

/** Builds the [[Evolutions]] of an ADT from its annotations only.
  *
  * It reads the annotations and generates the code to add these declarations in [[EvolutionBuilder]]. It compiles where
  * the ADT is visible, in its companion for example.
  */
private[api] object EvolutionMacro {

  def evolutionsImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Evolutions[T]] = {
    import c.universe._

    val tpe    = weakTypeOf[T]
    val symbol = tpe.typeSymbol
    // A case object has no companion: its sealed trait declares it
    val moduleChildren = descendantsOf(c)(symbol).filter(_.isModuleClass).distinct
    val builders       = tpe :: moduleChildren.map(_.asType.toType)
    c.Expr[Evolutions[T]](q"""
      new _root_.org.apache.flinkx.api.evolution.Evolutions[$tpe](
        () => _root_.scala.Seq[_root_.org.apache.flinkx.api.evolution.EvolutionBuilder[_]](..${builders.map(
        builderOf(c)
      )})
      )
    """)
  }

  /** Check that a versioned ADT has an evolving companion, where the type information of the ADT is derived. Give its
    * version to the derivation.
    */
  def evolvableImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Evolvable[T]] = {
    import c.universe._

    val tpe     = weakTypeOf[T]
    val symbol  = tpe.typeSymbol
    val version = versionOf(c)(symbol)

    rejectVersionedParameter(c)(symbol, primaryParameters(c)(tpe))
    // At version 0 an ADT has no evolution, and the sealed trait of a case object declares it
    if (version > 0 && !symbol.isModuleClass) {
      val companion = symbol.companion
      val evolving  = appliedType(typeOf[Evolving[_]].typeConstructor, tpe)
      if (companion == NoSymbol || !(companion.typeSignature <:< evolving)) {
        c.abort(c.enclosingPosition, companionNotEvolving(symbol.fullName))
      }
    }
    c.Expr[Evolvable[T]](q"new _root_.org.apache.flinkx.api.evolution.Evolvable[$tpe]($version)")
  }

  /** The subtypes of the given sealed type, through intermediate sealed traits. None for a class that is not sealed. */
  private def descendantsOf(c: blackbox.Context)(parent: c.Symbol): List[c.Symbol] =
    if (!parent.isClass) Nil
    else {
      val children = parent.asClass.knownDirectSubclasses.toList
      // Forces the signature: else a subtype of the same compilation unit can be unknown
      children.foreach(_.typeSignature)
      children ++ children.filter(child => child.isAbstract && child.asClass.isSealed).flatMap(descendantsOf(c))
    }

  /** The parameters of the primary constructor of the given type, or none for a trait. */
  private def primaryParameters(c: blackbox.Context)(tpe: c.Type): List[c.Symbol] = {
    import c.universe._
    tpe.decls
      .collectFirst { case method: MethodSymbol if method.isPrimaryConstructor => method }
      .toList
      .flatMap(_.paramLists.flatten)
  }

  /** Reject a `@version` on a parameter: only an ADT has a version, and the compiler does not check annotation targets.
    */
  private def rejectVersionedParameter(c: blackbox.Context)(symbol: c.Symbol, parameters: List[c.Symbol]): Unit =
    parameters
      .find(parameter => isVersioned(c)(parameter))
      .foreach(parameter =>
        c.abort(c.enclosingPosition, notAllowed("version", s"$symbol.${parameter.name.decodedName.toString}"))
      )

  /** Whether the given symbol declares a schema version of its own. */
  private def isVersioned(c: blackbox.Context)(symbol: c.Symbol): Boolean =
    try {
      symbol.info // Forces the symbol: else its annotations are not loaded
      symbol.annotations.exists(_.tree.tpe.typeSymbol == versionSymbol(c))
    } catch { case NonFatal(_) => false }

  /** The schema version that the `@version` of the given symbol declares, or 0 without `@version`. Rejects a version
    * that is negative or not an integer literal.
    */
  private def versionOf(c: blackbox.Context)(symbol: c.Symbol): Int =
    symbol.annotations.find(_.tree.tpe.typeSymbol == versionSymbol(c)).fold(0) { annotation =>
      val version = intArgument(c)(annotation, "current")
        .getOrElse(c.abort(c.enclosingPosition, notLiteral("version", "current", symbol.fullName)))
      if (version < 0) c.abort(c.enclosingPosition, versionNotAllowed(symbol.fullName, version))
      version
    }

  /** The integer literal that the given annotation passes to the named parameter, or as its first argument. */
  private def intArgument(c: blackbox.Context)(annotation: c.universe.Annotation, name: String): Option[Int] = {
    import c.universe._
    annotation.tree match {
      case Apply(_, args) =>
        args.collectFirst {
          case Literal(Constant(value: Int)) => value
          case NamedArg(Ident(argument), Literal(Constant(value: Int))) if argument.decodedName.toString == name =>
            value
        }
      case _ => None
    }
  }

  private def versionSymbol(c: blackbox.Context): c.Symbol =
    c.mirror.staticClass("org.apache.flinkx.api.evolution.version")

  /** The code that builds the [[EvolutionBuilder]] of the given ADT from its annotations, after their validation. */
  private def builderOf(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    val symbol = tpe.typeSymbol
    // The subtypes of a sealed trait, also through intermediate sealed traits
    val descendants = descendantsOf(c)(symbol)

    val parameters = primaryParameters(c)(tpe)

    // The fields of a case class, in declaration order. The snapshots of a sealed trait compare its subtypes
    val fieldNames = if (descendants.isEmpty) parameters.map(_.name.decodedName.toString) else Nil

    // Compare by symbol, not by type: the generic annotations have no type tag
    def annotationSymbol(name: String): Symbol = c.mirror.staticClass(s"org.apache.flinkx.api.evolution.$name")
    val Renamed                                = annotationSymbol("renamed")
    val DeletedClasses                         = annotationSymbol("deletedClasses")
    val PostEvolution                          = annotationSymbol("postEvolution")
    val Added                                  = annotationSymbol("added")

    val version = EvolutionMacro.versionOf(c)(symbol)

    def nameOf(annotation: Annotation): String = annotation.tree.tpe.typeSymbol.name.decodedName.toString

    def is(annotation: Annotation, annotationType: Symbol): Boolean =
      annotation.tree.tpe.typeSymbol == annotationType

    def isVersioned(owner: Symbol): Boolean = EvolutionMacro.isVersioned(c)(owner)

    // Compare by base class: the error type of an annotation that does not compile conforms to all types
    def evolutionAnnotationsOf(owner: Symbol): List[Annotation] =
      owner.annotations.filter(_.tree.tpe.baseClasses.contains(typeOf[EvolutionAnnotation].typeSymbol))

    /** Reject a `since` that is not an integer literal, or not from 1 to `lastVersion`. */
    def checkSince(annotation: Annotation, target: String, lastVersion: Int = version): Unit = {
      val since = intArgument(c)(annotation, "since")
        .getOrElse(c.abort(c.enclosingPosition, notLiteral(nameOf(annotation), "since", target)))
      // An evolution with a `since` out of this range never applies to the correct data
      if (since < 1 || since > lastVersion) {
        c.abort(c.enclosingPosition, sinceNotAllowed(symbol.fullName, since, lastVersion))
      }
    }

    /** Reject the misplaced evolution annotations, before the macro generates code. */
    def validate(): Unit = {
      rejectVersionedParameter(c)(symbol, parameters)
      val isCoproduct = descendants.nonEmpty

      def reject(annotation: Annotation, target: String): Nothing =
        c.abort(c.enclosingPosition, notAllowed(nameOf(annotation), target))

      evolutionAnnotationsOf(symbol).foreach { annotation =>
        if (!AnnotationRules.allowedOnAdt(nameOf(annotation), version, isCoproduct)) {
          reject(annotation, AnnotationRules.target(symbol.fullName, None, version))
        }
        // The `since` of a deleted class counts in the versions of this class, not in the versions of the ADT
        if (is(annotation, DeletedClasses)) checkSince(annotation, symbol.fullName, Int.MaxValue)
        else if (!is(annotation, PostEvolution)) checkSince(annotation, symbol.fullName)
      }

      if (symbol.annotations.count(annotation => is(annotation, PostEvolution)) > 1) {
        c.abort(c.enclosingPosition, notAllowed("postEvolution", s"${symbol.fullName} twice"))
      }

      parameters.foreach { parameter =>
        val field = parameter.name.decodedName.toString
        evolutionAnnotationsOf(parameter).foreach { annotation =>
          if (!AnnotationRules.allowedOnField(nameOf(annotation), version)) {
            reject(annotation, AnnotationRules.target(symbol.fullName, Some(field), version))
          }
          if (is(annotation, Added) && !parameter.asTerm.isParamWithDefault) {
            c.abort(c.enclosingPosition, addedFieldWithoutDefault(symbol.fullName, field))
          }
          checkSince(annotation, s"${symbol.fullName}.$field")
        }
      }

      // A versioned subtype has its own builder, which validates its own annotations
      descendants.filterNot(isVersioned).foreach { child =>
        evolutionAnnotationsOf(child).foreach { annotation =>
          if (!AnnotationRules.allowedOnUnversionedSubtype(nameOf(annotation), version, isEnum = false)) {
            reject(annotation, child.fullName)
          }
        }
      }
    }

    // The annotation as an untyped expression to splice: the compiler typed it in the context of the annotated class
    def instanceOf(annotation: Annotation): Tree = c.untypecheck(annotation.tree)

    validate()
    val builder           = TermName("builder")
    val classDeclarations = evolutionAnnotationsOf(symbol).map(a => q"$builder.declare(${instanceOf(a)})")
    val fieldDeclarations = parameters.zipWithIndex.flatMap { case (parameter, index) =>
      val name = parameter.name.decodedName.toString
      evolutionAnnotationsOf(parameter).map(a => q"$builder.declareField($name, $index, ${instanceOf(a)})")
    }
    q"""{
      val $builder = new _root_.org.apache.flinkx.api.evolution.EvolutionBuilder[$tpe](
        _root_.scala.reflect.classTag[$tpe].runtimeClass.asInstanceOf[_root_.java.lang.Class[$tpe]],
        $version,
        _root_.scala.Array(..$fieldNames)
      )
      ..$classDeclarations
      ..$fieldDeclarations
      $builder
    }"""
  }

}
