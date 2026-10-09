package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.{evolutionNotAllowed => notAllowed}

import scala.reflect.macros.blackbox
import scala.util.control.NonFatal

/** Builds the [[Evolutions]] of an ADT from its annotations alone.
  *
  * Reads what the derivation reads, but emits only the registration code: no field typeclass is summoned, so it
  * compiles wherever the ADT is visible, from its own companion in particular.
  */
private[api] object EvolutionMacro {

  /** The subtypes the derivation serializes, and the intermediate sealed traits it flattened on the way. */
  final case class Subtypes[S](serialized: List[S], intermediates: List[S])

  def evolutionsImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Evolutions[T]] = {
    import c.universe._

    val tpe    = weakTypeOf[T]
    val symbol = tpe.typeSymbol
    // A case object has no companion of its own to declare it: the sealed trait it belongs to declares it
    val (moduleChildren, classChildren) =
      if (symbol.isClass) subtypesOf(c)(symbol.asClass).serialized.partition(_.isModuleClass) else (Nil, Nil)
    val builders       = tpe :: moduleChildren.map(_.asType.toType)
    val subtypeClasses =
      classChildren.map(child => q"_root_.scala.reflect.classTag[${child.asType.toType}].runtimeClass")
    c.Expr[Evolutions[T]](q"""
      new _root_.org.apache.flinkx.api.evolution.Evolutions[$tpe](
        _root_.scala.reflect.classTag[$tpe].runtimeClass.asInstanceOf[_root_.java.lang.Class[$tpe]],
        () => _root_.scala.Seq[_root_.org.apache.flinkx.api.evolution.EvolutionBuilder[_]](..${builders.map(
        builderOf(c)
      )}),
        () => _root_.scala.Seq[_root_.java.lang.Class[_]](..$subtypeClasses)
      )
    """)
  }

  /** Check the companion of a versioned ADT extends [[Evolving]], wherever the type information of the ADT is derived.
    */
  def evolvableImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Evolvable[T]] = {
    import c.universe._

    val tpe    = weakTypeOf[T]
    val symbol = tpe.typeSymbol

    rejectVersionedParameter(c)(symbol, primaryParameters(c)(tpe))
    // A version 0 declares nothing to evolve from, and a case object is declared by the sealed trait it belongs to
    if (versionOf(c)(symbol) > 0 && !symbol.isModuleClass) {
      val companion = symbol.companion
      val evolving  = appliedType(typeOf[Evolving[_]].typeConstructor, tpe)
      if (companion == NoSymbol || !(companion.typeSignature <:< evolving)) {
        c.abort(c.enclosingPosition, companionNotEvolving(symbol.fullName))
      }
    }
    c.Expr[Evolvable[T]](q"new _root_.org.apache.flinkx.api.evolution.Evolvable[$tpe](${versionOf(c)(symbol)})")
  }

  /** The subtypes the derivation serializes, with the intermediate sealed traits it flattens on the way.
    *
    * Magnolia replaces a sealed subtype by its own subtypes and sorts them by name, so a declaration describing the
    * direct children instead would not match the serialized members, and an unchanged schema would be reported as
    * needing a migration.
    */
  private def subtypesOf(c: blackbox.Context)(parent: c.universe.ClassSymbol): Subtypes[c.Symbol] = {
    val (abstractChildren, concreteChildren) = parent.knownDirectSubclasses.toList.partition(_.isAbstract)
    // Forces the signature, without which a child of the same compilation unit may not be known yet
    (concreteChildren ++ abstractChildren).foreach(_.typeSignature)
    val flattened = abstractChildren.collect {
      case child if child.asClass.isSealed => subtypesOf(c)(child.asClass)
    }
    Subtypes(
      (concreteChildren ++ flattened.flatMap(_.serialized)).sortBy(_.fullName),
      abstractChildren ++ flattened.flatMap(_.intermediates)
    )
  }

  /** The parameters of the primary constructor of the given type, none for a trait. */
  private def primaryParameters(c: blackbox.Context)(tpe: c.Type): List[c.Symbol] = {
    import c.universe._
    tpe.decls
      .collectFirst { case method: MethodSymbol if method.isPrimaryConstructor => method }
      .toList
      .flatMap(_.paramLists.flatten)
  }

  /** Reject a `@version` on a parameter: a version belongs to an ADT, and scalac does not enforce annotation targets.
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
      symbol.info // Forces the symbol, without which its annotations are not loaded
      symbol.annotations.exists(_.tree.tpe.typeSymbol == versionSymbol(c))
    } catch { case NonFatal(_) => false }

  /** Schema version the `version` annotation of the given symbol declares, 0 when it declares none. Rejects a version
    * that is negative or not an integer literal.
    */
  private def versionOf(c: blackbox.Context)(symbol: c.Symbol): Int =
    symbol.annotations.find(_.tree.tpe.typeSymbol == versionSymbol(c)).fold(0) { annotation =>
      val version = intArgument(c)(annotation, "current")
        .getOrElse(c.abort(c.enclosingPosition, notLiteral("version", "current", symbol.fullName)))
      if (version < 0) c.abort(c.enclosingPosition, versionNotAllowed(symbol.fullName, version))
      version
    }

  /** The literal `Int` the given annotation passes to the named parameter, its first one, if it is a literal. */
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

  /** The code building the [[EvolutionBuilder]] of the given ADT type from its annotations. */
  private def builderOf(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    val evolution = q"_root_.org.apache.flinkx.api.evolution"
    val symbol    = tpe.typeSymbol
    // The subtypes the derivation serializes, and every sealed trait it flattened on the way, which declares nothing
    val Subtypes(children, intermediates) =
      if (symbol.isClass) subtypesOf(c)(symbol.asClass) else Subtypes[Symbol](Nil, Nil)

    val parameters = primaryParameters(c)(tpe)

    /** Members of the ADT, in declaration order: field names or subtype class names. */
    val memberNames: Tree =
      if (children.isEmpty) q"_root_.scala.Array(..${parameters.map(p => q"${p.name.decodedName.toString}")})"
      else {
        val subtypeClasses =
          children.map(child => q"_root_.scala.reflect.classTag[${child.asType.toType}].runtimeClass")
        q"_root_.scala.Array[_root_.java.lang.Class[_]](..$subtypeClasses).map(_.getName)"
      }

    // Compared by symbol rather than by type: the generic annotations have no type tag to compare against
    def annotationSymbol(name: String): Symbol = c.mirror.staticClass(s"org.apache.flinkx.api.evolution.$name")
    val Renamed                                = annotationSymbol("renamed")
    val DeletedFields                          = annotationSymbol("deletedFields")
    val DeletedClasses                         = annotationSymbol("deletedClasses")
    val PostEvolution                          = annotationSymbol("postEvolution")
    val Added                                  = annotationSymbol("added")
    val Transformed                            = annotationSymbol("transformed")

    val version = EvolutionMacro.versionOf(c)(symbol)

    def nameOf(annotation: Annotation): String = annotation.tree.tpe.typeSymbol.name.decodedName.toString

    /** The literal `since` of a field evolution, rejected outside the version range of the ADT. */
    def sinceOf(annotation: Annotation, target: String): Int = {
      val since = intArgument(c)(annotation, "since")
        .getOrElse(c.abort(c.enclosingPosition, notLiteral(nameOf(annotation), "since", target)))
      // An evolution outside the version range is never applied when it should
      if (since < 1 || since > version) c.abort(c.enclosingPosition, sinceNotAllowed(symbol.fullName, since, version))
      since
    }

    def is(annotation: Annotation, annotationType: Symbol): Boolean =
      annotation.tree.tpe.typeSymbol == annotationType

    def isVersioned(owner: Symbol): Boolean = EvolutionMacro.isVersioned(c)(owner)

    /** The annotation as an expression to splice, detyped: it was typed in the context of the class declaring it. */
    def instanceOf(annotation: Annotation): Tree = c.untypecheck(annotation.tree)

    def classAnnotations(builder: TermName, clazz: TermName): List[Tree] = symbol.annotations.collect {
      case a if is(a, Renamed) =>
        q"""val r = ${instanceOf(a)}; $builder.renameClass(r.formerName, r.since)"""
      case a if is(a, DeletedFields) =>
        val since = sinceOf(a, symbol.fullName)
        q"""${instanceOf(a)}.formerNames.foreach(name =>
              $builder.addFieldEvolution($evolution.FieldEvolution.Delete($since, $clazz, name))
            )"""
      case a if is(a, DeletedClasses) =>
        q"""val d = ${instanceOf(a)}
            d.formerClassNames.foreach(name => $builder.deleteClass(name, d.since, d.throwOnInstance))"""
      case a if is(a, PostEvolution) =>
        // The mapper is read on first use: the companion holding it may still be initializing
        val post = TermName(c.freshName("post$"))
        q"""lazy val $post = ${instanceOf(a)}.asInstanceOf[_root_.org.apache.flinkx.api.evolution.postEvolution[$tpe]]
            $builder.addPostEvolution(
              new _root_.org.apache.flinkx.api.evolution.postEvolution[$tpe]((v: _root_.scala.Int, i: $tpe) => $post.mapper(v, i))
            )"""
    }

    def fieldAnnotations(builder: TermName, clazz: TermName): List[Tree] =
      parameters.zipWithIndex.flatMap { case (parameter, index) =>
        val label  = parameter.name.decodedName.toString
        val target = s"${symbol.fullName}.$label"
        parameter.annotations.collect {
          case a if is(a, Added) =>
            if (!parameter.asTerm.isParamWithDefault) {
              c.abort(c.enclosingPosition, addedFieldWithoutDefault(symbol.fullName, label))
            }
            q"""$builder.addFieldEvolution($evolution.FieldEvolution.Add(${sinceOf(
                a,
                target
              )}, $clazz, $label, $index))"""
          case a if is(a, Renamed) =>
            q"""$builder.addFieldEvolution(
                  $evolution.FieldEvolution.Rename(${sinceOf(a, target)}, $clazz, ${instanceOf(a)}.formerName, $label)
                )"""
          case a if is(a, Transformed) =>
            // The mapper is read on first use: the companion holding it may still be initializing
            val List(from, to) = a.tree.tpe.typeArgs.map(TypeTree(_))
            val mapper         = TermName(c.freshName("transform$"))
            q"""lazy val $mapper = ${instanceOf(a)}
                $builder.addFieldEvolution($evolution.FieldEvolution.Transform[$from, $to](
                  ${sinceOf(a, target)}, $clazz, $label, (a: $from) => $mapper.mapper(a)
                ))"""
        }
      }

    /** Reject the evolution annotations that declare nothing where they are, before anything is generated. */
    def validate(): Unit = {
      val hasVersionedAncestor = tpe.baseClasses.filterNot(_ == symbol).exists(ancestor => isVersioned(ancestor))

      rejectVersionedParameter(c)(symbol, parameters)
      val isCoproduct = children.nonEmpty

      def reject(annotation: Annotation, target: String): Nothing =
        c.abort(c.enclosingPosition, notAllowed(nameOf(annotation), target))

      def isEvolutionAnnotation(annotation: Annotation): Boolean =
        annotation.tree.tpe <:< typeOf[EvolutionAnnotation]

      symbol.annotations.filter(isEvolutionAnnotation).foreach { annotation =>
        if (!AnnotationRules.allowedOnAdt(nameOf(annotation), version, isCoproduct, hasVersionedAncestor)) {
          reject(annotation, AnnotationRules.target(symbol.fullName, None, version))
        }
      }

      if (symbol.annotations.count(annotation => is(annotation, PostEvolution)) > 1) {
        c.abort(c.enclosingPosition, notAllowed("postEvolution", s"${symbol.fullName} twice"))
      }

      parameters.foreach { parameter =>
        parameter.annotations.filter(isEvolutionAnnotation).foreach { annotation =>
          if (!AnnotationRules.allowedOnField(nameOf(annotation), version)) {
            val field = parameter.name.decodedName.toString
            reject(annotation, AnnotationRules.target(symbol.fullName, Some(field), version))
          }
        }
      }

      // A subtype declares its own evolutions, which its own declaration registers
      (children ++ intermediates).filterNot(isVersioned).foreach { child =>
        child.annotations.filter(isEvolutionAnnotation).foreach { annotation =>
          if (!AnnotationRules.allowedOnUnversionedSubtype(nameOf(annotation), version, isEnum = false)) {
            reject(annotation, child.fullName)
          }
        }
      }
    }

    validate()
    val builder     = TermName("builder")
    val clazz       = TermName("declaredClass")
    val classOfTree = q"_root_.scala.reflect.classTag[$tpe].runtimeClass.asInstanceOf[_root_.java.lang.Class[$tpe]]"
    val classDeclarations = classAnnotations(builder, clazz)
    val fieldDeclarations = fieldAnnotations(builder, clazz)
    q"""{
      val $clazz = $classOfTree
      val $builder = new $evolution.EvolutionBuilder[$tpe]($clazz, $version, $memberNames)
      ..$classDeclarations
      ..$fieldDeclarations
      $builder
    }"""
  }

}
