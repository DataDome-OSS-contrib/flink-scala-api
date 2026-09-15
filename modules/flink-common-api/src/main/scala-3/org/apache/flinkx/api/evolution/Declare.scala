package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{DeletedReturnNull, DeletedThrowOnInstance, Renamed}
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.{
  AnnotationTrees,
  Evolved,
  added,
  deletedClasses,
  deletedFields,
  postDeserialize,
  renamed,
  transformed
}

import scala.quoted.*

/** Declares the evolutions of an ADT from its annotations alone.
  *
  * Reads what `TypeInformationDerivation` reads, but emits only the registration code: no field typeclass is summoned,
  * so it compiles wherever the ADT is visible, and it can be generated.
  */
object Declare:

  inline def declare[T]: Unit = ${ declareImpl[T] }

  private def declareImpl[T: Type](using Quotes): Expr[Unit] =
    import quotes.reflect.*

    val symbol   = TypeRepr.of[T].typeSymbol
    val isEnum   = symbol.flags.is(Flags.Enum)
    val children = symbol.children

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

    /** Members of the ADT, in declaration order: field names, enum value names or subtype class names. */
    val memberNames: Expr[Array[String]] =
      if children.isEmpty then Expr(parametersOf(symbol).map(_.name).toArray)
      else if isEnum then Expr(children.map(_.name).toArray)
      else
        // A case object child is a term, whose singleton type is what names its class
        val classNames = children.map(child => classOfExpr(if child.isTerm then child.termRef else child.typeRef))
        '{ Array(${ Varargs(classNames) }*).map(_.getName) }

    val VersionAnnotation = "org.apache.flinkx.api.version"

    def is[A: Type](term: Term): Boolean = term.tpe <:< TypeRepr.of[A]

    /** The annotations of a symbol, read whole so that splicing them is safe. */
    def annotationsOf(owner: Symbol): List[Term] = {
      AnnotationTrees.readWhole(List(owner))
      owner.annotations
    }

    /** Schema version declared by the `version` annotation, read from the tree: it is a Java annotation, which the
      * macro cannot instantiate, but whose argument is a constant.
      */
    def versionOf(owner: Symbol): Int =
      owner.annotations
        .find(_.tpe.typeSymbol.fullName == "org.apache.flinkx.api.version")
        .flatMap {
          case Apply(_, args) =>
            args.collectFirst {
              case Literal(IntConstant(declared))              => declared
              case NamedArg(_, Literal(IntConstant(declared))) => declared
            }
          case _ => None
        }
        .getOrElse(0)

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
        case term if is[postDeserialize[?]](term) =>
          term.tpe.typeArgs.head.asType match
            case '[a] =>
              val p = term.asExprOf[postDeserialize[a]]
              '{ $builder.addPostDeserialize($p.asInstanceOf[postDeserialize[T]]) }
      }

    def fieldAnnotations(builder: Expr[EvolutionBuilder[T]], clazz: Expr[Class[T]]): List[Expr[Unit]] =
      parametersOf(symbol).zipWithIndex.flatMap { case (parameter, index) =>
        val label = Expr(parameter.name)
        annotationsOf(parameter).collect {
          case term if is[added](term) =>
            val a = term.asExprOf[added]
            '{
              $builder.fieldEvolutions += Add(
                $a.since,
                $clazz,
                $label,
                Evolutions.defaultFieldValue($clazz, ${ Expr(index) })
              )
            }
          case term if is[renamed](term) =>
            val a = term.asExprOf[renamed]
            '{ val r = $a; $builder.fieldEvolutions += Rename(r.since, $clazz, r.formerName, $label) }
          case term if is[transformed[?, ?]](term) =>
            (term.tpe.typeArgs.head.asType, term.tpe.typeArgs.last.asType) match
              case ('[from], '[to]) =>
                val t = term.asExprOf[transformed[from, to]]
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
      val version     = versionOf(symbol)
      val isVersioned = (owner: Symbol) => owner.annotations.exists(_.tpe.typeSymbol.fullName == VersionAnnotation)
      val hasVersionedAncestor = TypeRepr.of[T].baseClasses.filterNot(_ == symbol).exists(isVersioned)
      val isCoproduct          = children.nonEmpty

      def reject(term: Term, target: String): Nothing =
        report.errorAndAbort(evolutionNotAllowed(term.tpe.typeSymbol.name, target))

      def isEvolved(term: Term): Boolean = term.tpe <:< TypeRepr.of[Evolved]

      symbol.annotations.filter(isEvolved).foreach { term =>
        val allowed =
          if version == 0 then is[renamed](term) && hasVersionedAncestor
          else
            is[renamed](term) || is[deletedClasses](term) || is[postDeserialize[?]](term) ||
            (is[deletedFields](term) && !isCoproduct)
        if !allowed then reject(term, if version == 0 then s"${symbol.fullName} with version 0" else symbol.fullName)
      }

      if symbol.annotations.count(is[postDeserialize[?]]) > 1 then
        report.errorAndAbort(evolutionNotAllowed("postDeserialize", s"${symbol.fullName} twice"))

      parametersOf(symbol).filter(_.annotations.exists(isEvolved)).foreach { parameter =>
        parameter.annotations.filter(isEvolved).foreach { term =>
          val allowed =
            version > 0 && (is[added](term) || is[renamed](term) || is[transformed[?, ?]](term))
          val target = s"${symbol.fullName}.${parameter.name}" + (if version == 0 then " with version 0" else "")
          if !allowed then reject(term, target)
        }
      }

      // A subtype declares its own evolutions, which its own declaration registers
      children.filterNot(isVersioned).foreach { child =>
        child.annotations.filter(isEvolved).foreach { term =>
          val allowed = isEnum && is[renamed](term) && version > 0
          if !allowed then reject(term, child.fullName)
        }
      }
    }

    validate()
    val clazz = '{ ${ classOfExpr(TypeRepr.of[T]) }.asInstanceOf[Class[T]] }
    '{
      val declaredClass = $clazz
      val builder       = new EvolutionBuilder[T](declaredClass, Evolutions.findVersion(declaredClass), $memberNames)
      ${ Expr.block(classAnnotations('builder, 'declaredClass), '{ () }) }
      ${ Expr.block(fieldAnnotations('builder, 'declaredClass), '{ () }) }
      ${ Expr.block(enumValueAnnotations('builder), '{ () }) }
      Evolutions.register(builder)
    }
