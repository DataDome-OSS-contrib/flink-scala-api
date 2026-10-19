package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.AnnotationTrees

import scala.quoted.*
import scala.util.control.NonFatal

/** Builds the [[Evolutions]] of an ADT from its annotations alone.
  *
  * Reads what `TypeInformationDerivation` reads, but emits only the registration code: no field typeclass is summoned,
  * so it compiles wherever the ADT is visible, from its own companion in particular.
  */
private[evolution] object EvolutionMacro:

  /** Check the companion of a versioned ADT extends [[Evolving]], wherever the type information of the ADT is derived,
    * and hand its version over to the derivation.
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
    // A case object has no companion of its own to declare it: the sealed traits above it declare it
    def caseObjectsBelow(owner: Symbol): List[Symbol] =
      if owner.flags.is(Flags.Enum) then Nil
      else owner.children.flatMap(child => if child.isTerm then List(child) else caseObjectsBelow(child))
    val moduleChildren = caseObjectsBelow(symbol).distinct
    val builders       = builderImpl[T] :: moduleChildren.map(child =>
      child.termRef.asType match
        case '[c] => builderImpl[c]
    )
    val clazz = classOfExpr(TypeRepr.of[T])
    '{ new Evolutions[T]($clazz.asInstanceOf[Class[T]], () => Seq(${ Varargs(builders) }*)) }

  /** The code building the [[EvolutionBuilder]] of `T` from its annotations, once they are validated. */
  private def builderImpl[T: Type](using Quotes): Expr[EvolutionBuilder[T]] =
    import quotes.reflect.*

    val symbol   = TypeRepr.of[T].typeSymbol
    val isEnum   = symbol.flags.is(Flags.Enum)
    val children = symbol.children
    val version  = versionOf(symbol)
    // An enum declares the evolutions of its values, while a subtype of a sealed trait declares its own
    val enumValues = if isEnum then children else Nil

    // The fields of a case class, in declaration order: a sealed trait or an enum compares its members in its snapshots
    val fieldNames = Expr(if children.isEmpty then parametersOf(symbol).map(_.name).toArray else Array.empty[String])

    def is[A: Type](term: Term): Boolean = term.tpe <:< TypeRepr.of[A]

    def nameOf(term: Term): String = term.tpe.typeSymbol.name

    /** The evolution annotations of a symbol, read whole so that splicing them is safe. */
    def evolutionAnnotationsOf(owner: Symbol): List[Term] = {
      AnnotationTrees.readWhole(List(owner))
      owner.annotations.filter(is[EvolutionAnnotation])
    }

    // A subtype is also renamed or deleted in the versions of the sealed traits above it, which resolve its former name
    val classVersion = (version :: TypeRepr.of[T].baseClasses.filter(isVersioned).map(versionOf)).max

    /** Reject a `since` that is not a literal, or outside the version range the annotation is declared in. */
    def checkSince(term: Term, target: String, lastVersion: Int = version): Unit =
      val since = intArgument(term, "since").getOrElse(report.errorAndAbort(notLiteral(nameOf(term), "since", target)))
      // An evolution outside the version range is never applied when it should
      if since < 1 || since > lastVersion then
        report.errorAndAbort(sinceNotAllowed(symbol.fullName, since, lastVersion))

    /** Reject the evolution annotations that declare nothing where they are, before anything is generated. */
    def validate(): Unit = {
      val hasVersionedAncestor = TypeRepr.of[T].baseClasses.filterNot(_ == symbol).exists(isVersioned)
      val isCoproduct          = children.nonEmpty

      def reject(term: Term, target: String): Nothing = report.errorAndAbort(evolutionNotAllowed(nameOf(term), target))

      rejectVersionedParameter(symbol)

      evolutionAnnotationsOf(symbol).foreach { term =>
        if !AnnotationRules.allowedOnAdt(nameOf(term), version, isCoproduct, hasVersionedAncestor) then
          reject(term, AnnotationRules.target(symbol.fullName, None, version))
        if is[renamed](term) || is[deletedClasses](term) && !isEnum then checkSince(term, symbol.fullName, classVersion)
        else if !is[postEvolution[?]](term) then checkSince(term, symbol.fullName)
      }

      if symbol.annotations.count(is[postEvolution[?]]) > 1 then
        report.errorAndAbort(evolutionNotAllowed("postEvolution", s"${symbol.fullName} twice"))

      parametersOf(symbol).foreach { parameter =>
        evolutionAnnotationsOf(parameter).foreach { term =>
          if !AnnotationRules.allowedOnField(nameOf(term), version) then
            reject(term, AnnotationRules.target(symbol.fullName, Some(parameter.name), version))
          if is[added](term) && !parameter.flags.is(Flags.HasDefault) then
            report.errorAndAbort(addedFieldWithoutDefault(symbol.fullName, parameter.name))
          checkSince(term, s"${symbol.fullName}.${parameter.name}")
        }
      }

      // A subtype of a sealed trait declares its own evolutions, which its own declaration registers
      children.filter(child => isEnum || !isVersioned(child)).foreach { child =>
        evolutionAnnotationsOf(child).foreach { term =>
          if !AnnotationRules.allowedOnUnversionedSubtype(nameOf(term), version, isEnum) then
            reject(term, child.fullName)
          checkSince(term, s"${symbol.fullName}.${child.name}")
        }
      }
    }

    /** Hands the annotations over to the builder, which reads them at runtime. */
    def declarations(builder: Expr[EvolutionBuilder[T]]): List[Expr[Unit]] =
      def annotation(term: Term) = term.asExprOf[EvolutionAnnotation]
      val ofClass  = evolutionAnnotationsOf(symbol).map(term => '{ $builder.declare(${ annotation(term) }) })
      val ofFields = parametersOf(symbol).zipWithIndex.flatMap { case (parameter, index) =>
        val (name, position) = (Expr(parameter.name), Expr(index))
        evolutionAnnotationsOf(parameter).map(term =>
          '{ $builder.declareField($name, $position, ${ annotation(term) }) }
        )
      }
      val ofEnumValues = enumValues.flatMap { value =>
        evolutionAnnotationsOf(value).map(term =>
          '{ $builder.declareEnumValue(${ Expr(value.name) }, ${ annotation(term) }) }
        )
      }
      ofClass ++ ofFields ++ ofEnumValues

    validate()
    val clazz = classOfExpr(TypeRepr.of[T])
    '{
      val builder =
        new EvolutionBuilder[T]($clazz.asInstanceOf[Class[T]], ${ Expr(version) }, $fieldNames, ${ Expr(isEnum) })
      ${ Expr.block(declarations('builder), '{ () }) }
      builder
    }
