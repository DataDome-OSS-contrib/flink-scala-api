package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.AnnotationTrees

import scala.quoted.*
import scala.util.control.NonFatal

/** Builds the [[Evolutions]] of an ADT from its annotations alone.
  *
  * Reads what `TypeInformationDerivation` reads, but emits only the registration code: no field typeclass is summoned,
  * so it compiles wherever the ADT is visible, from its own companion in particular.
  */
private[evolution] object EvolutionMacro:

  /** Check the companion of a versioned ADT extends [[Evolving]], wherever the type information of the ADT is derived.
    */
  def evolvableImpl[T: Type](using Quotes): Expr[Evolvable[T]] =
    import quotes.reflect.*

    val symbol = TypeRepr.of[T].typeSymbol

    rejectVersionedParameter(symbol)
    // A version 0 declares nothing to evolve from, and a case object is declared by the sealed trait it belongs to
    if versionOf(symbol) > 0 && !symbol.flags.is(Flags.Module) then
      // An enum value is typed by its enum, which declares for it
      val companion = Option.when(symbol.companionModule.exists)(symbol.companionModule.termRef)
      val evolving  = Symbol.requiredClass(classOf[Evolving[?]].getName).typeRef.appliedTo(symbol.typeRef)
      if !companion.exists(_ <:< evolving) then report.errorAndAbort(companionNotEvolving(symbol.fullName))
    // An enum value case class is not versioned on its own: it's serialized with the version of its enum
    val enumOfValue =
      if symbol.flags.is(Flags.Enum) && symbol.flags.is(Flags.Case) then
        TypeRepr.of[T].baseClasses.find(base => base != symbol && base.flags.is(Flags.Enum))
      else None
    val version = Expr(versionOf(enumOfValue.getOrElse(symbol)))
    '{ new Evolvable[T]($version) }

  /** Whether the given symbol declares a schema version of its own. */
  private def isVersioned(using q: Quotes)(symbol: q.reflect.Symbol): Boolean = {
    import q.reflect.*
    try symbol.annotations.exists(_.tpe <:< TypeRepr.of[version])
    catch { case NonFatal(_) => false }
  }

  /** The value parameters of the primary constructor of the given class, none for a trait. */
  private def parametersOf(using q: Quotes)(owner: q.reflect.Symbol): List[q.reflect.Symbol] =
    owner.primaryConstructor match
      case constructor if constructor.isNoSymbol => Nil
      case constructor                           => constructor.paramSymss.filterNot(_.exists(_.isTypeParam)).flatten

  /** Reject a `@version` on a parameter: a version belongs to an ADT, and scalac does not enforce annotation targets.
    */
  private def rejectVersionedParameter(using q: Quotes)(symbol: q.reflect.Symbol): Unit =
    parametersOf(symbol)
      .find(isVersioned)
      .foreach(parameter =>
        q.reflect.report.errorAndAbort(evolutionNotAllowed("version", s"$symbol.${parameter.name}"))
      )

  /** The class of the given type, from its class tag. */
  private def classOfExpr(using q: Quotes)(tpe: q.reflect.TypeRepr): Expr[Class[?]] =
    import q.reflect.*
    tpe.asType match
      case '[t] =>
        Expr
          .summon[scala.reflect.ClassTag[t]]
          .map(tag => '{ $tag.runtimeClass })
          .getOrElse(report.errorAndAbort(s"No ClassTag for ${tpe.show}"))

  /** Schema version the `version` annotation of the given symbol declares, 0 when it declares none. Rejects a version
    * that is negative or not an integer literal.
    */
  private def versionOf(using q: Quotes)(symbol: q.reflect.Symbol): Int = {
    import q.reflect.*
    symbol.annotations.find(_.tpe <:< TypeRepr.of[version]).fold(0) { annotation =>
      val version = intArgument(annotation, "current")
        .getOrElse(report.errorAndAbort(notLiteral("version", "current", symbol.fullName)))
      if version < 0 then report.errorAndAbort(versionNotAllowed(symbol.fullName, version))
      version
    }
  }

  /** The literal `Int` the given annotation passes to the named parameter, its first one, if it is a literal. */
  private def intArgument(using q: Quotes)(annotation: q.reflect.Term, name: String): Option[Int] = {
    import q.reflect.*
    annotation match
      case Apply(_, args) =>
        args.collectFirst {
          case Literal(IntConstant(value))                                         => value
          case NamedArg(argument, Literal(IntConstant(value))) if argument == name => value
        }
      case _ => None
  }

  def evolutionsImpl[T: Type](using Quotes): Expr[Evolutions[T]] =
    import quotes.reflect.*

    val symbol = TypeRepr.of[T].typeSymbol
    // A case object has no companion of its own to declare it: the sealed trait it belongs to declares it
    val moduleChildren = if symbol.flags.is(Flags.Enum) then Nil else symbol.children.filter(_.isTerm)
    val builders       = builderImpl[T] :: moduleChildren.map(child =>
      child.termRef.asType match
        case '[c] => builderImpl[c]
    )
    val subtypeClasses = if symbol.flags.is(Flags.Enum) then Nil else symbol.children.filterNot(_.isTerm).map(_.typeRef)
    val classes        = subtypeClasses.map(classOfExpr)
    val clazz          = classOfExpr(TypeRepr.of[T])
    '{
      new Evolutions[T](
        $clazz.asInstanceOf[Class[T]],
        () => Seq(${ Varargs(builders) }*),
        () => Seq(${ Varargs(classes) }*)
      )
    }

  /** The code building the [[EvolutionBuilder]] of `T` from its annotations. */
  private def builderImpl[T: Type](using Quotes): Expr[EvolutionBuilder[T]] =
    import quotes.reflect.*

    val symbol   = TypeRepr.of[T].typeSymbol
    val isEnum   = symbol.flags.is(Flags.Enum)
    val children = symbol.children
    val version  = versionOf(symbol)

    /** The subtypes of a sealed trait, a case object child being a term whose singleton type names its class. */
    val subtypeClasses: List[Expr[Class[?]]] =
      if isEnum then Nil
      else children.map(child => classOfExpr(if child.isTerm then child.termRef else child.typeRef))

    /** Members of the ADT, in declaration order: field names, enum value names or subtype class names. */
    val memberNames: Expr[Array[String]] =
      if children.isEmpty then Expr(parametersOf(symbol).map(_.name).toArray)
      else if isEnum then Expr(children.map(_.name).toArray)
      else '{ Array(${ Varargs(subtypeClasses) }*).map(_.getName) }

    def is[A: Type](term: Term): Boolean = term.tpe <:< TypeRepr.of[A]

    def nameOf(term: Term): String = term.tpe.typeSymbol.name

    /** The literal `since` of a field evolution, rejected outside the version range of the ADT. */
    def sinceOf(term: Term, target: String): Expr[Int] =
      val since = intArgument(term, "since").getOrElse(report.errorAndAbort(notLiteral(nameOf(term), "since", target)))
      // An evolution outside the version range is never applied when it should
      if since < 1 || since > version then report.errorAndAbort(sinceNotAllowed(symbol.fullName, since, version))
      Expr(since)

    /** The annotations of a symbol, read whole so that splicing them is safe. */
    def annotationsOf(owner: Symbol): List[Term] = {
      AnnotationTrees.readWhole(List(owner))
      owner.annotations
    }

    def classAnnotations(builder: Expr[EvolutionBuilder[T]], clazz: Expr[Class[T]]): List[Expr[Unit]] =
      annotationsOf(symbol).collect {
        case term if is[renamed](term) =>
          val a = term.asExprOf[renamed]
          '{ val r = $a; $builder.renameClass(r.formerName, r.since) }
        case term if is[deletedFields](term) =>
          val a     = term.asExprOf[deletedFields]
          val since = sinceOf(term, symbol.fullName)
          '{ $a.formerNames.foreach(name => $builder.addFieldEvolution(Delete($since, $clazz, name))) }
        case term if is[deletedClasses](term) && isEnum =>
          val a = term.asExprOf[deletedClasses]
          '{
            val d = $a
            d.formerClassNames.foreach(name => $builder.deleteEnumValue(name, d.throwOnInstance))
          }
        case term if is[deletedClasses](term) =>
          val a = term.asExprOf[deletedClasses]
          '{
            val d = $a
            d.formerClassNames.foreach(name => $builder.deleteClass(name, d.since, d.throwOnInstance))
          }
        case term if is[postEvolution[?]](term) =>
          term.tpe.typeArgs.head.asType match
            case '[a] =>
              val p = term.asExprOf[postEvolution[a]]
              // The mapper is read on first use: the companion holding it may still be initializing
              '{
                lazy val post = $p
                $builder.addPostEvolution(
                  new postEvolution[T]((v: Int, i: T) => post.mapper(v, i.asInstanceOf[a]).asInstanceOf[T])
                )
              }
      }

    def fieldAnnotations(builder: Expr[EvolutionBuilder[T]], clazz: Expr[Class[T]]): List[Expr[Unit]] =
      parametersOf(symbol).zipWithIndex.flatMap { case (parameter, index) =>
        val label  = Expr(parameter.name)
        val target = s"${symbol.fullName}.${parameter.name}"
        annotationsOf(parameter).collect {
          case term if is[added](term) =>
            if !parameter.flags.is(Flags.HasDefault) then
              report.errorAndAbort(addedFieldWithoutDefault(symbol.fullName, parameter.name))
            val since = sinceOf(term, target)
            '{ $builder.addFieldEvolution(Add($since, $clazz, $label, ${ Expr(index) })) }
          case term if is[renamed](term) =>
            val a     = term.asExprOf[renamed]
            val since = sinceOf(term, target)
            '{ $builder.addFieldEvolution(Rename($since, $clazz, $a.formerName, $label)) }
          case term if is[transformed[?, ?]](term) =>
            (term.tpe.typeArgs.head.asType, term.tpe.typeArgs.last.asType) match
              case ('[from], '[to]) =>
                val t     = term.asExprOf[transformed[from, to]]
                val since = sinceOf(term, target)
                // The mapper is read on first use: the companion holding it may still be initializing
                '{
                  lazy val tr = $t
                  $builder.addFieldEvolution(Transform[from, to]($since, $clazz, $label, (a: from) => tr.mapper(a)))
                }
        }
      }

    /** Value renames of a Scala 3 enum, declared on its values rather than on the enum itself. */
    def enumValueAnnotations(builder: Expr[EvolutionBuilder[T]]): List[Expr[Unit]] =
      if !isEnum then Nil
      else
        children.flatMap { child =>
          val valueName = Expr(child.name)
          annotationsOf(child).collect {
            case term if is[renamed](term) =>
              val a = term.asExprOf[renamed]
              '{ $builder.renameEnumValue($a.formerName, $valueName) }
          }
        }

    /** Reject the evolution annotations that declare nothing where they are, before anything is generated. */
    def validate(): Unit = {
      val hasVersionedAncestor = TypeRepr.of[T].baseClasses.filterNot(_ == symbol).exists(isVersioned)
      val isCoproduct          = children.nonEmpty

      def reject(term: Term, target: String): Nothing = report.errorAndAbort(evolutionNotAllowed(nameOf(term), target))

      rejectVersionedParameter(symbol)

      def isEvolutionAnnotation(term: Term): Boolean = term.tpe <:< TypeRepr.of[EvolutionAnnotation]

      symbol.annotations.filter(isEvolutionAnnotation).foreach { term =>
        if !AnnotationRules.allowedOnAdt(nameOf(term), version, isCoproduct, hasVersionedAncestor) then
          reject(term, AnnotationRules.target(symbol.fullName, None, version))
      }

      if symbol.annotations.count(is[postEvolution[?]]) > 1 then
        report.errorAndAbort(evolutionNotAllowed("postEvolution", s"${symbol.fullName} twice"))

      parametersOf(symbol).filter(_.annotations.exists(isEvolutionAnnotation)).foreach { parameter =>
        parameter.annotations.filter(isEvolutionAnnotation).foreach { term =>
          if !AnnotationRules.allowedOnField(nameOf(term), version) then
            reject(term, AnnotationRules.target(symbol.fullName, Some(parameter.name), version))
        }
      }

      // A subtype declares its own evolutions, which its own declaration registers
      children.filterNot(isVersioned).foreach { child =>
        child.annotations.filter(isEvolutionAnnotation).foreach { term =>
          if !AnnotationRules.allowedOnUnversionedSubtype(nameOf(term), version, isEnum) then
            reject(term, child.fullName)
        }
      }
    }

    validate()
    val clazz = classOfExpr(TypeRepr.of[T])
    '{
      val declaredClass = $clazz.asInstanceOf[Class[T]]
      val builder       = new EvolutionBuilder[T](declaredClass, ${ Expr(version) }, $memberNames)
      ${ Expr.block(classAnnotations('builder, 'declaredClass), '{ () }) }
      ${ Expr.block(fieldAnnotations('builder, 'declaredClass), '{ () }) }
      ${ Expr.block(enumValueAnnotations('builder), '{ () }) }
      builder
    }
