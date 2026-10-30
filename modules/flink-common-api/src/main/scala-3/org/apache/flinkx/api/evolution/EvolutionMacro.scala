package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.AnnotationTrees

import scala.quoted.*
import scala.util.control.NonFatal

/** Builds the [[Evolutions]] of an ADT from its annotations only.
  *
  * It reads the annotations and generates the code to add these declarations in [[EvolutionBuilder]]. It compiles where
  * the ADT is visible, in its companion for example.
  */
private[evolution] object EvolutionMacro:

  /** Check that a versioned ADT has an evolving companion, where the type information of the ADT is derived. Give its
    * version to the derivation.
    */
  def evolvableImpl[T: Type](using Quotes): Expr[Evolvable[T]] =
    import quotes.reflect.*

    val symbol = TypeRepr.of[T].typeSymbol

    rejectVersionedParameter(symbol)
    // At version 0 an ADT has no evolution, and the sealed trait of a case object declares it
    if versionOf(symbol) > 0 && !symbol.flags.is(Flags.Module) then
      // An enum value has the type of its enum, which declares its evolutions
      val companion = Option.when(symbol.companionModule.exists)(symbol.companionModule.termRef)
      val evolving  = Symbol.requiredClass(classOf[Evolving[?]].getName).typeRef.appliedTo(symbol.typeRef)
      if !companion.exists(_ <:< evolving) then report.errorAndAbort(companionNotEvolving(symbol.fullName))
    // An enum value with parameters has no version of its own: its snapshot records the version of its enum
    val version = Expr(versionOf(enumOf(TypeRepr.of[T]).getOrElse(symbol)))
    '{ new Evolvable[T]($version) }

  /** The enum of the given enum value with parameters, or none for another type. */
  private def enumOf(using q: Quotes)(tpe: q.reflect.TypeRepr): Option[q.reflect.Symbol] =
    import q.reflect.*
    val symbol = tpe.typeSymbol
    if symbol.flags.is(Flags.Enum) && symbol.flags.is(Flags.Case) then
      tpe.baseClasses.find(base => base != symbol && base.flags.is(Flags.Enum))
    else None

  /** Whether the given symbol declares a schema version of its own. */
  private def isVersioned(using q: Quotes)(symbol: q.reflect.Symbol): Boolean = {
    import q.reflect.*
    try symbol.annotations.exists(_.tpe <:< TypeRepr.of[version])
    catch { case NonFatal(_) => false }
  }

  /** The value parameters of the primary constructor of the given class, or none for a trait. */
  private def parametersOf(using q: Quotes)(owner: q.reflect.Symbol): List[q.reflect.Symbol] =
    owner.primaryConstructor match
      case constructor if constructor.isNoSymbol => Nil
      case constructor                           => constructor.paramSymss.filterNot(_.exists(_.isTypeParam)).flatten

  /** Reject a `@version` on a parameter: only an ADT has a version, and the compiler does not check annotation targets.
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

  /** The schema version that the `@version` of the given symbol declares, or 0 without `@version`. Rejects a version
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

  /** The integer literal that the given annotation passes to the named parameter, or as its first argument. */
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
    // A case object has no companion: its sealed traits declare it
    def caseObjectsBelow(owner: Symbol): List[Symbol] =
      if owner.flags.is(Flags.Enum) then Nil
      else owner.children.flatMap(child => if child.isTerm then List(child) else caseObjectsBelow(child))
    // An enum value with parameters has a class of its own, which the snapshot of its fields records
    val enumValuesWithParameters = if symbol.flags.is(Flags.Enum) then symbol.children.filterNot(_.isTerm) else Nil
    // A generic enum value has wildcard type arguments: its class does not depend on them
    def typeOf(value: Symbol): TypeRepr =
      value.primaryConstructor.paramSymss.flatten.filter(_.isTypeParam) match
        case Nil        => value.typeRef
        case typeParams => value.typeRef.appliedTo(typeParams.map(_ => TypeBounds.empty))
    def builders(adt: Expr[EvolutionBuilder[T]]): List[Expr[EvolutionBuilder[?]]] =
      caseObjectsBelow(symbol).distinct.map(_.termRef.asType).map { case '[c] => builderImpl[c](None) } ++
        enumValuesWithParameters.map(typeOf(_).asType).map { case '[c] => builderImpl[c](Some(adt)) }
    '{
      new Evolutions[T](() => {
        val adt = ${ builderImpl[T](None) }
        adt +: ${ Varargs(builders('adt)) }
      })
    }

  /** The code that builds the [[EvolutionBuilder]] of `T` from its annotations, after their validation.
    * @param enumBuilder
    *   The builder of the enum of `T`, if `T` is an enum value with parameters
    */
  private def builderImpl[T: Type](enumBuilder: Option[Expr[EvolutionBuilder[?]]])(using
      Quotes
  ): Expr[EvolutionBuilder[T]] =
    import quotes.reflect.*

    val symbol = TypeRepr.of[T].typeSymbol
    // An enum value with parameters has the version of its enum
    val enumOfValue = enumOf(TypeRepr.of[T])
    val isEnum      = symbol.flags.is(Flags.Enum) && enumOfValue.isEmpty
    val children    = symbol.children
    val version     = versionOf(enumOfValue.getOrElse(symbol))
    // An enum declares the evolutions of its values. A subtype of a sealed trait declares its own evolutions
    val enumValues = if isEnum then children else Nil

    // The fields of a case class, in declaration order. The snapshots of a sealed trait or an enum compare its members
    val fieldNames = Expr(if children.isEmpty then parametersOf(symbol).map(_.name).toArray else Array.empty[String])

    def is[A: Type](term: Term): Boolean = term.tpe <:< TypeRepr.of[A]

    def nameOf(term: Term): String = term.tpe.typeSymbol.name

    /** The evolution annotations of a symbol, read whole: a splice of them is then safe. */
    def evolutionAnnotationsOf(owner: Symbol): List[Term] = {
      AnnotationTrees.readWhole(List(owner))
      owner.annotations.filter(is[EvolutionAnnotation])
    }

    // A rename or deletion of a subtype can count in the versions of its sealed traits, which record its name
    val classVersion = (version :: TypeRepr.of[T].baseClasses.filter(isVersioned).map(versionOf)).max

    /** Reject a `since` that is not an integer literal, or not from 1 to `lastVersion`. */
    def checkSince(term: Term, target: String, lastVersion: Int = version): Unit =
      val since = intArgument(term, "since").getOrElse(report.errorAndAbort(notLiteral(nameOf(term), "since", target)))
      // An evolution with a `since` out of this range never applies to the correct data
      if since < 1 || since > lastVersion then
        report.errorAndAbort(sinceNotAllowed(symbol.fullName, since, lastVersion))

    /** Reject the misplaced evolution annotations, before the macro generates code. */
    def validate(): Unit = {
      val isCoproduct = children.nonEmpty

      def reject(term: Term, target: String): Nothing = report.errorAndAbort(evolutionNotAllowed(nameOf(term), target))

      rejectVersionedParameter(symbol)

      evolutionAnnotationsOf(symbol).foreach { term =>
        if !AnnotationRules.allowedOnAdt(nameOf(term), version, isCoproduct) then
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

      // A versioned subtype or an enum value with parameters has its own builder, which validates its own annotations
      children.filter(child => if isEnum then child.isTerm else !isVersioned(child)).foreach { child =>
        evolutionAnnotationsOf(child).foreach { term =>
          if !AnnotationRules.allowedOnUnversionedSubtype(nameOf(term), version, isEnum) then
            reject(term, child.fullName)
          checkSince(term, s"${symbol.fullName}.${child.name}")
        }
      }
    }

    /** Give the annotations to the builder, which reads them at runtime. */
    def declarations(builder: Expr[EvolutionBuilder[T]]): List[Expr[Unit]] =
      def annotation(term: Term) = term.asExprOf[EvolutionAnnotation]
      val ofClass  = evolutionAnnotationsOf(symbol).map(term => '{ $builder.declare(${ annotation(term) }) })
      val ofFields = parametersOf(symbol).zipWithIndex.flatMap { case (parameter, index) =>
        val (name, position) = (Expr(parameter.name), Expr(index))
        evolutionAnnotationsOf(parameter).map(term =>
          '{ $builder.declareField($name, $position, ${ annotation(term) }) }
        )
      }
      // The builder of an enum value with parameters declares its other annotations
      val ofEnumValues = enumValues.flatMap { value =>
        evolutionAnnotationsOf(value)
          .filter(is[renamed])
          .map(term => '{ $builder.declareEnumValue(${ Expr(value.name) }, ${ annotation(term) }) })
      }
      ofClass ++ ofFields ++ ofEnumValues

    validate()
    val clazz    = classOfExpr(TypeRepr.of[T])
    val enclosed = enumBuilder.fold('{ None })(enumBuilder => '{ Some($enumBuilder) })
    '{
      val builder = new EvolutionBuilder[T](
        $clazz.asInstanceOf[Class[T]],
        ${ Expr(version) },
        $fieldNames,
        ${ Expr(isEnum) },
        $enclosed
      )
      ${ Expr.block(declarations('builder), '{ () }) }
      builder
    }
