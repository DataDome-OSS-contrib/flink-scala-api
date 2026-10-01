package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{DeletedReturnNull, DeletedThrowOnInstance, Renamed}
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.AnnotationTrees

import scala.quoted.*

/** Builds the [[Declaration]] of an ADT from its annotations alone.
  *
  * Reads what `TypeInformationDerivation` reads, but emits only the registration code: no field typeclass is summoned,
  * so it compiles wherever the ADT is visible, from its own companion in particular.
  */
private[evolution] object Declare:

  /** Whether the given symbol declares a schema version of its own. */
  def isVersioned(using q: Quotes)(symbol: q.reflect.Symbol): Boolean = {
    import q.reflect.*
    try symbol.annotations.exists(_.tpe <:< TypeRepr.of[version])
    catch { case _: Throwable => false }
  }

  /** Schema version the `version` annotation of the given symbol declares, 0 when it declares none. */
  def versionOf(using q: Quotes)(symbol: q.reflect.Symbol): Int = {
    import q.reflect.*
    symbol.annotations.find(_.tpe <:< TypeRepr.of[version]).flatMap(intArgument(_, "current")).getOrElse(0)
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

  def declarationImpl[T: Type](using Quotes): Expr[Declaration[T]] =
    import quotes.reflect.*

    val symbol = TypeRepr.of[T].typeSymbol
    // A case object has no companion of its own to declare it: the sealed trait it belongs to declares it
    val moduleChildren = if symbol.flags.is(Flags.Enum) then Nil else symbol.children.filter(_.isTerm)
    val builders       = builderImpl[T] :: moduleChildren.map(child =>
      child.termRef.asType match
        case '[c] => builderImpl[c]
    )
    val subtypeClasses = if symbol.flags.is(Flags.Enum) then Nil else symbol.children.filterNot(_.isTerm).map(_.typeRef)
    val classes        = subtypeClasses.map(tpe =>
      tpe.asType match
        case '[t] =>
          Expr
            .summon[scala.reflect.ClassTag[t]]
            .map(tag => '{ $tag.runtimeClass })
            .getOrElse(report.errorAndAbort(s"No ClassTag for ${tpe.show}"))
    )
    val clazz =
      Expr.summon[scala.reflect.ClassTag[T]].getOrElse(report.errorAndAbort(s"No ClassTag for ${TypeRepr.of[T].show}"))
    '{
      new Declaration[T](
        $clazz.runtimeClass.asInstanceOf[Class[T]],
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

    def classOfExpr(tpe: TypeRepr): Expr[Class[?]] =
      tpe.asType match
        case '[t] =>
          Expr
            .summon[scala.reflect.ClassTag[t]]
            .map(tag => '{ $tag.runtimeClass })
            .getOrElse(report.errorAndAbort(s"No ClassTag for ${tpe.show}"))

    def parametersOf(owner: Symbol): List[Symbol] =
      owner.primaryConstructor match
        case constructor if constructor.isNoSymbol => Nil
        case constructor                           => constructor.paramSymss.filterNot(_.exists(_.isTypeParam)).flatten

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

    /** The annotations of a symbol, read whole so that splicing them is safe. */
    def annotationsOf(owner: Symbol): List[Term] = {
      AnnotationTrees.readWhole(List(owner))
      owner.annotations
    }

    def classAnnotations(builder: Expr[EvolutionBuilder[T]], clazz: Expr[Class[T]]): List[Expr[Unit]] =
      annotationsOf(symbol).collect {
        case term if is[renamed](term) =>
          val a = term.asExprOf[renamed]
          '{ val r = $a; $builder.registerFormerClass(r.formerName, $clazz, r.since) }
        case term if is[deletedFields](term) =>
          val a = term.asExprOf[deletedFields]
          '{
            val d = $a
            d.formerNames.foreach(name => $builder.fieldEvolutions += Delete(d.since, $clazz, name))
          }
        case term if is[deletedClasses](term) && isEnum =>
          val a = term.asExprOf[deletedClasses]
          '{
            val d       = $a
            val deleted = if (d.throwOnInstance) DeletedThrowOnInstance else DeletedReturnNull
            d.formerClassNames.foreach(name => $builder.formerEnumValues(name) = deleted)
          }
        case term if is[deletedClasses](term) =>
          val a = term.asExprOf[deletedClasses]
          '{
            val d = $a
            d.formerClassNames.foreach(name =>
              $builder.registerDeletedFormerClass(name, $clazz, d.since, d.throwOnInstance)
            )
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
        val label = Expr(parameter.name)
        annotationsOf(parameter).collect {
          case term if is[added](term) =>
            if !parameter.flags.is(Flags.HasDefault) then
              report.errorAndAbort(addedFieldWithoutDefault(symbol.fullName, parameter.name))
            val a = term.asExprOf[added]
            '{
              $builder.fieldEvolutions += Add(
                $a.since,
                $clazz,
                $label,
                ${ Expr(index) }
              )
            }
          case term if is[renamed](term) =>
            val a = term.asExprOf[renamed]
            '{ val r = $a; $builder.fieldEvolutions += Rename(r.since, $clazz, r.formerName, $label) }
          case term if is[transformed[?, ?]](term) =>
            (term.tpe.typeArgs.head.asType, term.tpe.typeArgs.last.asType) match
              case ('[from], '[to]) =>
                val t = term.asExprOf[transformed[from, to]]
                intArgument(term, "since") match
                  // The mapper is read on first use: the companion holding it may still be initializing
                  case Some(since) =>
                    '{
                      lazy val tr = $t
                      $builder.fieldEvolutions +=
                        Transform[from, to](${ Expr(since) }, $clazz, $label, (a: from) => tr.mapper(a))
                    }
                  case None =>
                    '{ val tr = $t; $builder.fieldEvolutions += Transform(tr.since, $clazz, $label, tr.mapper) }
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
              '{ $builder.formerEnumValues($a.formerName) = Renamed($valueName) }
          }
        }

    /** Reject the evolution annotations that declare nothing where they are, before anything is generated. */
    def validate(): Unit = {
      val hasVersionedAncestor = TypeRepr.of[T].baseClasses.filterNot(_ == symbol).exists(isVersioned)
      val isCoproduct          = children.nonEmpty

      def reject(term: Term, target: String): Nothing =
        report.errorAndAbort(evolutionNotAllowed(term.tpe.typeSymbol.name, target))

      // A version belongs to an ADT, and scalac does not enforce the target of an annotation
      parametersOf(symbol)
        .find(isVersioned)
        .foreach(parameter => report.errorAndAbort(evolutionNotAllowed("version", s"$symbol.${parameter.name}")))

      def isEvolutionAnnotation(term: Term): Boolean = term.tpe <:< TypeRepr.of[EvolutionAnnotation]

      symbol.annotations.filter(isEvolutionAnnotation).foreach { term =>
        val allowed =
          if version == 0 then is[renamed](term) && hasVersionedAncestor
          else
            is[renamed](term) || is[deletedClasses](term) || is[postEvolution[?]](term) ||
            (is[deletedFields](term) && !isCoproduct)
        if !allowed then reject(term, if version == 0 then s"${symbol.fullName} with version 0" else symbol.fullName)
      }

      if symbol.annotations.count(is[postEvolution[?]]) > 1 then
        report.errorAndAbort(evolutionNotAllowed("postEvolution", s"${symbol.fullName} twice"))

      parametersOf(symbol).filter(_.annotations.exists(isEvolutionAnnotation)).foreach { parameter =>
        parameter.annotations.filter(isEvolutionAnnotation).foreach { term =>
          val allowed =
            version > 0 && (is[added](term) || is[renamed](term) || is[transformed[?, ?]](term))
          val target = s"${symbol.fullName}.${parameter.name}" + (if version == 0 then " with version 0" else "")
          if !allowed then reject(term, target)
        }
      }

      // A subtype declares its own evolutions, which its own declaration registers
      children.filterNot(isVersioned).foreach { child =>
        child.annotations.filter(isEvolutionAnnotation).foreach { term =>
          val allowed = isEnum && is[renamed](term) && version > 0
          if !allowed then reject(term, child.fullName)
        }
      }
    }

    validate()
    '{
      val declaredClass = ${ classOfExpr(TypeRepr.of[T]) }.asInstanceOf[Class[T]]
      val builder       = new EvolutionBuilder[T](declaredClass, ${ Expr(version) }, $memberNames)
      ${ Expr.block(classAnnotations('builder, 'declaredClass), '{ () }) }
      ${ Expr.block(fieldAnnotations('builder, 'declaredClass), '{ () }) }
      ${ Expr.block(enumValueAnnotations('builder), '{ () }) }
      builder
    }
