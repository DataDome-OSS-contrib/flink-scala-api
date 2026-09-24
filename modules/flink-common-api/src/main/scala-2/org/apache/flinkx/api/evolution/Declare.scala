package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.{evolutionNotAllowed => notAllowed}

import scala.reflect.macros.blackbox

/** Builds the [[Declaration]] of an ADT from its annotations alone.
  *
  * Reads what the derivation reads, but emits only the registration code: no field typeclass is summoned, so it
  * compiles wherever the ADT is visible, from its own companion in particular.
  */
private[api] object Declare {

  def declarationImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[Declaration[T]] = {
    import c.universe._

    val tpe    = weakTypeOf[T]
    val symbol = tpe.typeSymbol
    // A case object has no companion of its own to declare it: the sealed trait it belongs to declares it
    val (moduleChildren, classChildren) =
      if (symbol.isClass) subtypesOf(c)(symbol.asClass)._1.partition(_.isModuleClass) else (Nil, Nil)
    val builders       = tpe :: moduleChildren.map(_.asType.toType)
    val subtypeClasses =
      classChildren.map(child => q"_root_.scala.reflect.classTag[${child.asType.toType}].runtimeClass")
    c.Expr[Declaration[T]](q"""
      new _root_.org.apache.flinkx.api.evolution.Declaration[$tpe](
        _root_.scala.reflect.classTag[$tpe].runtimeClass.asInstanceOf[_root_.java.lang.Class[$tpe]],
        () => _root_.scala.Seq[_root_.org.apache.flinkx.api.evolution.EvolutionBuilder[_]](..${builders.map(
        builderOf(c)
      )}),
        () => _root_.scala.Seq[_root_.java.lang.Class[_]](..$subtypeClasses)
      )
    """)
  }

  /** The subtypes the derivation serializes, with the intermediate sealed traits it flattens on the way.
    *
    * Magnolia replaces a sealed subtype by its own subtypes and sorts them by name, so a declaration describing the
    * direct children instead would not match the serialized members, and an unchanged schema would be reported as
    * needing a migration.
    */
  def subtypesOf(c: blackbox.Context)(parent: c.universe.ClassSymbol): (List[c.Symbol], List[c.Symbol]) = {
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
  def isVersioned(c: blackbox.Context)(symbol: c.Symbol): Boolean =
    try {
      symbol.info // Forces the symbol, without which its annotations are not loaded
      symbol.annotations.exists(_.tree.tpe.typeSymbol == versionSymbol(c))
    } catch { case _: Throwable => false }

  /** Schema version the `version` annotation of the given symbol declares, 0 when it declares none. */
  def versionOf(c: blackbox.Context)(symbol: c.Symbol): Int =
    symbol.annotations
      .find(_.tree.tpe.typeSymbol == versionSymbol(c))
      .flatMap(intArgument(c)(_, "current"))
      .getOrElse(0)

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

  private def versionSymbol(c: blackbox.Context): c.Symbol = c.mirror.staticClass("org.apache.flinkx.api.version")

  /** The code building the [[EvolutionBuilder]] of the given ADT type from its annotations. */
  private def builderOf(c: blackbox.Context)(tpe: c.Type): c.Tree = {
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
      else {
        val subtypeClasses =
          children.map(child => q"_root_.scala.reflect.classTag[${child.asType.toType}].runtimeClass")
        q"_root_.scala.Array[_root_.java.lang.Class[_]](..$subtypeClasses).map(_.getName)"
      }

    // Compared by symbol rather than by type: the generic annotations have no type tag to compare against
    def annotationSymbol(name: String): Symbol = c.mirror.staticClass(s"org.apache.flinkx.api.$name")
    val Renamed                                = annotationSymbol("renamed")
    val DeletedFields                          = annotationSymbol("deletedFields")
    val DeletedClasses                         = annotationSymbol("deletedClasses")
    val PostEvolution                          = annotationSymbol("postEvolution")
    val Added                                  = annotationSymbol("added")
    val Transformed                            = annotationSymbol("transformed")

    def versionOf(owner: Symbol): Int = Declare.versionOf(c)(owner)

    def is(annotation: Annotation, annotationType: Symbol): Boolean =
      annotation.tree.tpe.typeSymbol == annotationType

    def isVersioned(owner: Symbol): Boolean = Declare.isVersioned(c)(owner)

    // The companion the declaration is written in, a case object being its own
    val module: Symbol = if (symbol.isModuleClass) symbol.asClass.module else symbol.companion
    val self           = TermName(c.freshName("self$"))
    var selfReferenced = false

    /** Replaces the references to the companion by a value reached by name once the companion is initialized.
      *
      * The declaration is spliced in the parent constructor call of the companion, where scalac forbids referencing the
      * companion itself, even from a lambda: a mapper of the companion, the natural place for it, would not compile
      * otherwise.
      */
    object DetachSelf extends Transformer {
      override def transform(tree: Tree): Tree = tree match {
        case _
            if module != NoSymbol && tree.symbol == module && (tree.isInstanceOf[Ident] || tree.isInstanceOf[Select]) =>
          selfReferenced = true
          Ident(self)
        case This(_) if module != NoSymbol && tree.symbol == module.asModule.moduleClass =>
          selfReferenced = true
          Ident(self)
        case _ => super.transform(tree)
      }
    }

    /** The annotation as an expression to splice, detyped: it was typed in the context of the class declaring it. */
    def instanceOf(annotation: Annotation): Tree = c.untypecheck(DetachSelf.transform(annotation.tree))

    def classAnnotations(builder: TermName, clazz: TermName): List[Tree] = symbol.annotations.collect {
      case a if is(a, Renamed) =>
        q"""val r = ${instanceOf(a)}; $builder.registerFormerClass(r.formerName, $clazz, r.since)"""
      case a if is(a, DeletedFields) =>
        q"""val d = ${instanceOf(a)}
            d.formerNames.foreach(name => $builder.fieldEvolutions += $evolution.FieldEvolution.Delete(d.since, $clazz, name))"""
      case a if is(a, DeletedClasses) =>
        q"""val d = ${instanceOf(a)}
            d.formerClassNames.foreach(name => $builder.registerDeletedFormerClass(name, $clazz, d.since, d.throwOnInstance))"""
      case a if is(a, PostEvolution) =>
        // The mapper is read on first use: the companion holding it may still be initializing
        val post = TermName(c.freshName("post$"))
        q"""lazy val $post = ${instanceOf(a)}.asInstanceOf[_root_.org.apache.flinkx.api.postEvolution[$tpe]]
            $builder.addPostEvolution(
              new _root_.org.apache.flinkx.api.postEvolution[$tpe]((v: _root_.scala.Int, i: $tpe) => $post.mapper(v, i))
            )"""
    }

    def fieldAnnotations(builder: TermName, clazz: TermName): List[Tree] =
      parameters.zipWithIndex.flatMap { case (parameter, index) =>
        val label = parameter.name.decodedName.toString
        parameter.annotations.collect {
          case a if is(a, Added) =>
            if (!parameter.asTerm.isParamWithDefault) {
              c.abort(c.enclosingPosition, addedFieldWithoutDefault(symbol.fullName, label))
            }
            q"""$builder.fieldEvolutions += $evolution.FieldEvolution.Add(${instanceOf(
                a
              )}.since, $clazz, $label, () => _root_.org.apache.flinkx.api.util.ClassUtil.defaultFieldValue($clazz, $index))"""
          case a if is(a, Renamed) =>
            q"""val r = ${instanceOf(a)}
                $builder.fieldEvolutions += $evolution.FieldEvolution.Rename(r.since, $clazz, r.formerName, $label)"""
          case a if is(a, Transformed) =>
            intArgument(c)(a, "since") match {
              // The mapper is read on first use: the companion holding it may still be initializing
              case Some(since) =>
                val List(from, to) = a.tree.tpe.typeArgs.map(TypeTree(_))
                val mapper         = TermName(c.freshName("transform$"))
                q"""lazy val $mapper = ${instanceOf(a)}
                    $builder.fieldEvolutions += $evolution.FieldEvolution.Transform[$from, $to](
                      $since, $clazz, $label, (a: $from) => $mapper.mapper(a)
                    )"""
              case None =>
                q"""val t = ${instanceOf(a)}
                    $builder.fieldEvolutions += $evolution.FieldEvolution.Transform(t.since, $clazz, $label, t.mapper)"""
            }
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
            is(annotation, Renamed) || is(annotation, DeletedClasses) || is(annotation, PostEvolution) ||
            (is(annotation, DeletedFields) && !isCoproduct)
        if (!allowed) {
          reject(annotation, if (version == 0) s"${symbol.fullName} with version 0" else symbol.fullName)
        }
      }

      if (symbol.annotations.count(annotation => is(annotation, PostEvolution)) > 1) {
        c.abort(c.enclosingPosition, notAllowed("postEvolution", s"${symbol.fullName} twice"))
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
    val builder     = TermName("builder")
    val clazz       = TermName("declaredClass")
    val classOfTree = q"_root_.scala.reflect.classTag[$tpe].runtimeClass.asInstanceOf[_root_.java.lang.Class[$tpe]]"
    // Generated before the self value below, which they tell whether it is needed
    val classDeclarations = classAnnotations(builder, clazz)
    val fieldDeclarations = fieldAnnotations(builder, clazz)
    val selfDeclaration   =
      if (!selfReferenced) Nil
      else {
        val moduleType = TypeTree(module.typeSignature)
        List(
          // Resolved at runtime because a reference to an object from its own parent constructor is not allowed
          q"lazy val $self: $moduleType = _root_.org.apache.flinkx.api.util.ClassUtil.companionInstance[$moduleType]($clazz)"
        )
      }
    q"""{
      val $clazz = $classOfTree
      ..$selfDeclaration
      val $builder = new $evolution.EvolutionBuilder[$tpe]($clazz, ${versionOf(symbol)}, $memberNames)
      ..$classDeclarations
      ..$fieldDeclarations
      $builder
    }"""
  }

}
