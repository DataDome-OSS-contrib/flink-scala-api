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

  /** Declares every versioned ADT of the given package and of its sub-packages, nested ones included.
    *
    * Covers the package as the compiler sees it, dependencies contributing classes to it included. An ADT living in a
    * package of its own is left to the provider of its module, or named by a `declare` of its own.
    */
  def declarePackage(packageName: String): Unit = macro declarePackageImpl

  /** Declares every versioned ADT of the package this call is written in, and of its sub-packages.
    *
    * Same as naming that package, so a provider sitting at the root of a model declares it whole.
    */
  def declarePackage: Unit = macro declareEnclosingPackageImpl

  def declareImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Unit] = {
    import c.universe._
    c.Expr[Unit](declareType(c)(weakTypeOf[T]))
  }

  def declarePackageImpl(c: blackbox.Context)(packageName: c.Expr[String]): c.Expr[Unit] = {
    import c.universe._

    val name = packageName.tree match {
      case Literal(Constant(literal: String)) => literal
      case tree => c.abort(c.enclosingPosition, s"declarePackage expects a literal package name, got $tree")
    }
    c.Expr[Unit](declarationsOf(c)(name))
  }

  def declareEnclosingPackageImpl(c: blackbox.Context): c.Expr[Unit] = {
    import c.universe._

    var owner = c.internal.enclosingOwner
    while (owner != NoSymbol && !owner.isPackage && !owner.isPackageClass) owner = owner.owner
    val name = if (owner == NoSymbol) "" else owner.fullName
    // Scanning every package of the classpath forces signatures a dependency may not bring, which fails the build
    if (name.isEmpty || name == "<empty>") {
      c.abort(
        c.enclosingPosition,
        "declarePackage must be called from a package, as the root package cannot be scanned"
      )
    }
    c.Expr[Unit](declarationsOf(c)(name))
  }

  private def declarationsOf(c: blackbox.Context)(packageName: String): c.Tree = {
    import c.universe._

    val adts = versionedIn(c)(packageName)
    if (adts.isEmpty) c.warning(c.enclosingPosition, s"No @version annotated ADT found in package '$packageName'")
    val declarations = adts.map(adt => declareType(c)(adtType(c)(adt)))
    q"{ ..$declarations }"
  }

  /** The subtypes the derivation serializes, with the intermediate sealed traits it flattens on the way.
    *
    * Magnolia replaces a sealed subtype by its own subtypes and sorts them by name, so a declaration describing the
    * direct children instead would not match the serialized members, and an unchanged schema would be reported as
    * needing a migration.
    */
  private def subtypesOf(c: blackbox.Context)(parent: c.universe.ClassSymbol): (List[c.Symbol], List[c.Symbol]) = {
    val (abstractChildren, concreteChildren) = parent.knownDirectSubclasses.toList.partition(_.isAbstract)
    // Forces the signature, without which a child of the same compilation unit may not be known yet
    (concreteChildren ++ abstractChildren).foreach(_.typeSignature)
    val flattened = abstractChildren.collect {
      case child if child.asClass.isSealed => subtypesOf(c)(child.asClass)
    }
    (
      (concreteChildren ++ flattened.flatMap(_._1)).sortBy(_.fullName),
      abstractChildren ++ flattened.flatMap(_._2)
    )
  }

  /** Whether the given symbol declares a schema version of its own. */
  private def isVersioned(c: blackbox.Context)(symbol: c.Symbol): Boolean =
    try {
      symbol.info // Forces the symbol, without which its annotations are not loaded
      symbol.annotations.exists(_.tree.tpe.typeSymbol == versionSymbol(c))
    } catch { case _: Throwable => false }

  /** Schema version the `version` annotation of the given symbol declares, 0 when it declares none. */
  private def versionOf(c: blackbox.Context)(symbol: c.Symbol): Int = {
    import c.universe._
    symbol.annotations
      .find(_.tree.tpe.typeSymbol == versionSymbol(c))
      .flatMap(_.tree match {
        case Apply(_, args) =>
          args.collectFirst {
            case Literal(Constant(declared: Int))              => declared
            case NamedArg(_, Literal(Constant(declared: Int))) => declared
          }
        case _ => None
      })
      .getOrElse(0)
  }

  private def versionSymbol(c: blackbox.Context): c.Symbol = c.mirror.staticClass("org.apache.flinkx.api.version")

  /** Symbols of the versioned ADTs of the given package and of its sub-packages, nested in objects or not. */
  private def versionedIn(c: blackbox.Context)(packageName: String): List[c.Symbol] = {
    val root =
      try c.mirror.staticPackage(packageName)
      catch {
        case _: Throwable => c.abort(c.enclosingPosition, s"Cannot read package '$packageName': no such package")
      }

    def membersOf(owner: c.Symbol): List[c.Symbol] =
      try owner.info.decls.toList.filter(member => member.isClass || member.isModule)
      catch { case _: Throwable => Nil }

    def walk(owner: c.Symbol): List[c.Symbol] = {
      val members = membersOf(owner)
      members.filter(member => isVersioned(c)(member)) ++ members.flatMap(walk)
    }

    walk(root).distinct
  }

  /** The type of the given ADT symbol, a case object being a module whose singleton type names its class. */
  private def adtType(c: blackbox.Context)(symbol: c.Symbol): c.Type =
    if (symbol.isModule) symbol.asModule.info else symbol.asType.toType

  private def declareType(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    val evolution = q"_root_.org.apache.flinkx.api.evolution"
    val symbol    = tpe.typeSymbol
    // The subtypes the derivation serializes, and every sealed trait it flattened on the way, which declares nothing
    val (children, intermediates) =
      if (symbol.isClass) subtypesOf(c)(symbol.asClass) else (Nil, Nil)

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

    def versionOf(owner: Symbol): Int = Declare.versionOf(c)(owner)

    def is(annotation: Annotation, annotationType: Symbol): Boolean =
      annotation.tree.tpe.typeSymbol == annotationType

    def isVersioned(owner: Symbol): Boolean = Declare.isVersioned(c)(owner)

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
              )}.since, $clazz, $label, _root_.org.apache.flinkx.api.util.ClassUtil.defaultFieldValue($clazz, $index))"""
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

      // A version belongs to an ADT, and scalac does not enforce the target of an annotation
      parameters
        .find(parameter => isVersioned(parameter))
        .foreach(parameter =>
          c.abort(c.enclosingPosition, notAllowed("version", s"$symbol.${parameter.name.decodedName.toString}"))
        )
      val isCoproduct = children.nonEmpty

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
      (children ++ intermediates).filterNot(isVersioned).foreach { child =>
        child.annotations.filter(isEvolved).foreach(annotation => reject(annotation, child.fullName))
      }
    }

    validate()
    val builder = TermName("builder")
    val clazz   = TermName("declaredClass")
    q"""
      val $clazz   = _root_.scala.reflect.classTag[$tpe].runtimeClass.asInstanceOf[_root_.java.lang.Class[$tpe]]
      val $builder = new $evolution.EvolutionBuilder[$tpe]($clazz, ${versionOf(symbol)}, $memberNames)
      ..${classAnnotations(builder, clazz)}
      ..${fieldAnnotations(builder, clazz)}
      $evolution.Evolutions.register($builder)
    """
  }

}
