package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.{EvolutionBuilder, Evolutions}
import org.apache.flinkx.api.evolution.Evolutions.throwEvolutionNotAllowed
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.serializer.{
  CaseClassSerializer,
  CoproductSerializer,
  Scala3EnumSerializer,
  Scala3EnumValueSerializer,
  ScalaCaseObjectSerializer,
  nullable
}
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.IArray.genericWrapArray
import scala.collection.immutable.SortedSet
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
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val useCache = typeTag.isCachable
    val cacheKey = typeTag.toString
    (if useCache then cache.get(cacheKey) else None) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T & Product]]
        val version    = ctx.annotations.collectFirst(Evolutions.findVersion(clazz)).getOrElse(0)
        val fieldNames = ctx.parameters.map(_.label).toArray
        val serializer =
          if typeTag.isEnum then
            new Scala3EnumValueSerializer[T & Product](typeTag.companion.get.runtimeClass, ctx.typeInfo.short)
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

        val builder = EvolutionBuilder[T & Product](clazz, fieldNames) // Field names required even with version 0
        if version == 0 then
          // Do not allow Evolution annotations without a version
          ctx.annotations.collect {
            case _: renamed if ctx.inheritedAnnotations.exists(_.isInstanceOf[version]) => // Allow @renamed if parent has version
            case e: Evolved => throwEvolutionNotAllowed(e, s"$clazz without version")
          }
          ctx.parameters.foreach { p =>
            p.annotations.collect { case e: Evolved => throwEvolutionNotAllowed(e, s"$p of $clazz without version") }
          }
        else // version > 0
          // Iterate over case class annotations to register evolutions on current source code
          ctx.annotations.collect {
            case r: renamed         => Evolutions.registerFormerClass(r.from, clazz)
            case d: deletedFields   => d.names.foreach(builder.fieldEvolutions += Delete(d.since, clazz, _))
            case d: deletedClasses  => d.names.foreach(Evolutions.registerDeletedFormerClass(_, clazz))
            case p: postDeserialize[T & Product] => builder.postDeserialize = p.mapper
            case e: Evolved                      => throwEvolutionNotAllowed(e, clazz.toString)
          }
          // Iterate over case class fields annotations to register evolutions from current source code
          ctx.parameters.foreach { p =>
            p.annotations.collect {
              case a: added             => builder.fieldEvolutions += Add(a.since, clazz, p.label, p.default)
              case r: renamed           => builder.fieldEvolutions += Rename(r.since, clazz, p.label, r.from)
              case t: transformed[_, _] => builder.fieldEvolutions += Transform(t.since, clazz, p.label, t.mapper)
              case e: version           => throwEvolutionNotAllowed(e, s"$clazz.${p.label}")
              case e: Evolved           => throwEvolutionNotAllowed(e, s"$clazz.${p.label}")
            }
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
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val useCache = typeTag.isCachable
    val cacheKey = typeTag.toString
    (if useCache then cache.get(cacheKey) else None) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T]]
        val version    = ctx.annotations.collectFirst(Evolutions.findVersion(clazz)).getOrElse(0)
        val serializer =
          if typeTag.isEnum then
            new Scala3EnumSerializer[T & Product](
              clazz = clazz.asInstanceOf[Class[T & Product]],
              enumValueNames = ctx.subtypes.map(_.typeInfo.short).toArray,
              enumValueSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
            ).asInstanceOf[TypeSerializer[T]]
          else
            new CoproductSerializer[T](
              clazz = clazz,
              subtypeClasses = ctx.subtypes.map(_.typeclass.getTypeClass).toArray,
              subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
            )

        if version == 0 then
          // Do not allow Evolution annotations without a version
          ctx.annotations.collect { case e: Evolved => throwEvolutionNotAllowed(e, s"$clazz without version") }
          ctx.subtypes.foreach { p =>
            p.annotations.collect { case e: Evolved => throwEvolutionNotAllowed(e, s"$p of $clazz without version") }
          }
        else // version > 0
          val builder = EvolutionBuilder[T](clazz)
          // Iterate over coproduct annotations to register evolutions from current source code
          ctx.annotations.collect {
            case r: renamed            => Evolutions.registerFormerClass(r.from, clazz)
            case d: deletedClasses     => d.names.foreach(Evolutions.registerDeletedFormerClass(_, clazz))
            case p: postDeserialize[T] => builder.postDeserialize = p.mapper
            case e: Evolved            => throwEvolutionNotAllowed(e, clazz.toString)
          }
          // Iterate over subtypes annotations to register evolutions from current source code
          ctx.subtypes.foreach { p =>
            p.annotations.collect {
              case r: renamed        => Evolutions.registerFormerClass(r.from, p.typeclass.getTypeClass)
              case _: deletedClasses => // allowed on subtype
              case e: Evolved        => throwEvolutionNotAllowed(e, p.typeclass.getTypeClass.toString)
            }
          }
          Evolutions.register(builder)

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        if useCache then cache.put(cacheKey, ti)
        ti
