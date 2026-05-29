package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.evolution.dsl.{EvolvedMerger, OptionalEvolved}
import org.apache.flinkx.api.evolution.{EvolutionBuilder, EvolutionNotAllowedException, Evolutions}
import org.apache.flinkx.api.serializer.*
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.IArray.genericWrapArray
import scala.collection.mutable
import scala.reflect.ClassTag

private[api] trait TypeInformationDerivation extends TaggedDerivation[TypeInformation]:

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  protected val cache: mutable.Map[String, TypeInformation[?]] = mutable.Map.empty

  // We cannot add a constraint of `T <: Product`, even though `join` is always called on products.
  // Need to mix in via `& Product`.
  override def join[T](ctx: CaseClass[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T],
      optionalEvolved: OptionalEvolved[T]
  ): Typeclass[T] =
    val useCache = typeTag.isCachable
    val cacheKey = typeTag.toString
    (if useCache then cache.get(cacheKey) else None) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T & Product]]
        val evolvedDsl = optionalEvolved.value
        val version    = Evolutions.findVersion(clazz, ctx.annotations, evolvedDsl)
        val fieldNames = ctx.parameters.map(_.label).toArray
        val serializer =
          if typeTag.isEnum then new Scala3EnumValueSerializer[T & Product](clazz, ctx.typeInfo.short)
          else if typeTag.isModule then new ScalaCaseObjectSerializer[T & Product](clazz)
          else
            new CaseClassSerializer[T & Product](
              clazz = clazz,
              isCaseClassImmutable = isCaseClassImmutable(clazz, fieldNames),
              version = version,
              fieldNames = fieldNames,
              paramSerializers = ctx.params.map { p =>
                val ser = p.typeclass.createSerializer(config)
                if (p.annotations.exists(_.isInstanceOf[nullable])) {
                  NullableSerializer.wrapIfNullIsNotSupported(ser, true)
                } else ser
              }.toArray
            )

        val builder = new EvolutionBuilder[T & Product](clazz, fieldNames) // Field names required even with version 0
        if version == 0 then
          // Do not allow Evolution annotations on version 0
          ctx.annotations.foreach {
            case _: renamed if ctx.inheritedAnnotations.exists(_.isInstanceOf[version]) => // Allow @renamed if parent has version
            case e: Evolved => throw EvolutionNotAllowedException(e, s"$clazz with version 0")
            case _          => // Ignore other annotations
          }
          ctx.parameters.foreach { p =>
            p.annotations.foreach {
              case e: Evolved => throw EvolutionNotAllowedException(e, s"$p of $clazz with version 0")
              case _          => // Ignore other annotations
            }
          }
        else // version > 0
          // Iterate over case class annotations to register evolutions on current source code
          ctx.annotations.foreach {
            case r: renamed        => Evolutions.registerFormerClass(r.formerName, clazz)
            case d: deletedFields  => d.formerNames.foreach(builder.fieldEvolutions += Delete(d.since, clazz, _))
            case d: deletedClasses =>
              d.formerClassNames.foreach(Evolutions.registerDeletedFormerClass(_, clazz, d.throwOnInstance))
            case p: postDeserialize[T & Product] => builder.addPostDeserialize(p)
            case e: Evolved                      => throw EvolutionNotAllowedException(e, clazz.toString)
            case _                               => // Ignore other annotations
          }
          // Iterate over case class fields annotations to register evolutions from current source code
          ctx.parameters.foreach { p =>
            p.annotations.foreach {
              case a: added             => builder.fieldEvolutions += Add(a.since, clazz, p.label, p.default)
              case r: renamed           => builder.fieldEvolutions += Rename(r.since, clazz, r.formerName, p.label)
              case t: transformed[_, _] => builder.fieldEvolutions += Transform(t.since, clazz, p.label, t.mapper)
              case e: version           => throw EvolutionNotAllowedException(e, s"$clazz.${p.label}")
              case e: Evolved           => throw EvolutionNotAllowedException(e, s"$clazz.${p.label}")
              case _                    => // Ignore other annotations
            }
          }
          // Merge DSL-supplied evolutions (defaults sourced from Magnolia's `Param.default`).
          evolvedDsl.foreach { ev =>
            EvolvedMerger.merge[T & Product](
              // Safe cast: the DSL was declared for `Evolved[T]`, but `T & Product` shares the same runtime class.
              ev.asInstanceOf[org.apache.flinkx.api.evolution.dsl.Evolved[T & Product]],
              builder,
              clazz,
              name => ctx.parameters.find(_.label == name).flatMap(_.default)
            )
          }
        Evolutions.register(builder)

        val ti = new CaseClassTypeInfo[T & Product](
          clazz = clazz,
          fieldTypes = ctx.params.map(_.typeclass),
          fieldNames = fieldNames,
          ser = serializer
        ).asInstanceOf[TypeInformation[T]]
        if useCache then cache.put(cacheKey, ti)
        ti

  override def split[T](ctx: SealedTrait[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T],
      optionalEvolved: OptionalEvolved[T]
  ): Typeclass[T] =
    val useCache = typeTag.isCachable
    val cacheKey = typeTag.toString
    (if useCache then cache.get(cacheKey) else None) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T]]
        val evolvedDsl = optionalEvolved.value
        val version    = Evolutions.findVersion(clazz, ctx.annotations, evolvedDsl)
        val serializer =
          if typeTag.isEnum then
            new Scala3EnumSerializer[T & Product](
              clazz = clazz.asInstanceOf[Class[T & Product]],
              version = version,
              enumValueNames = ctx.subtypes.map(_.typeInfo.short).toArray,
              enumValueSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
            ).asInstanceOf[TypeSerializer[T]]
          else
            val subtypeClasses: Array[Class[?]] = ctx.subtypes.map(_.typeclass.getTypeClass).toArray
            new CoproductSerializer[T](
              clazz = clazz,
              version = version,
              subtypeClasses = subtypeClasses,
              subtypeFqns = subtypeClasses.map(_.getName),
              subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
            )

        if version == 0 then
          // Do not allow Evolution annotations on version 0
          ctx.annotations.foreach {
            case e: Evolved => throw EvolutionNotAllowedException(e, s"$clazz with version 0")
            case _          => // Ignore other annotations
          }
          ctx.subtypes.foreach { p =>
            p.annotations.foreach {
              case e: Evolved => throw EvolutionNotAllowedException(e, s"$p of $clazz with version 0")
              case _          => // Ignore other annotations
            }
          }
        else // version > 0
          val builder = new EvolutionBuilder[T](clazz)
          // Iterate over coproduct annotations to register evolutions from current source code
          ctx.annotations.foreach {
            case r: renamed                          => Evolutions.registerFormerClass(r.formerName, clazz)
            case d: deletedClasses if typeTag.isEnum =>
              d.formerClassNames.foreach(n =>
                Evolutions.registerDeletedFormerClass(s"${clazz.getSimpleName}#$n", clazz, d.throwOnInstance)
              )
            case d: deletedClasses =>
              d.formerClassNames.foreach(Evolutions.registerDeletedFormerClass(_, clazz, d.throwOnInstance))
            case p: postDeserialize[T] => builder.addPostDeserialize(p)
            case e: Evolved            => throw EvolutionNotAllowedException(e, clazz.toString)
            case _                     => // Ignore other annotations
          }
          // Iterate over subtypes annotations to register evolutions from current source code
          ctx.subtypes.foreach { p =>
            p.annotations.foreach {
              case r: renamed if typeTag.isEnum => builder.formerToCurrentEnumValueName(r.formerName) = p.typeInfo.short
              case r: renamed => Evolutions.registerFormerClass(r.formerName, p.typeclass.getTypeClass)
              case _: deletedFields if p.annotations.exists(_.isInstanceOf[version])  => // allowed on versioned subtype
              case _: deletedClasses if p.annotations.exists(_.isInstanceOf[version]) => // allowed on versioned subtype
              case _: postDeserialize[T] if p.annotations.exists(_.isInstanceOf[version]) => // allowed on versioned subtype
              case e: Evolved => throw EvolutionNotAllowedException(e, p.typeclass.getTypeClass.toString)
              case _          => // Ignore other annotations
            }
          }
          // Merge DSL-supplied evolutions for the sealed trait / enum.
          evolvedDsl.foreach { ev => EvolvedMerger.merge[T](ev, builder, clazz, _ => None) }
          Evolutions.register(builder)

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        if useCache then cache.put(cacheKey, ti)
        ti
