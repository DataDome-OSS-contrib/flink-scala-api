package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.evolution.{Evolution, Evolutions}
import org.apache.flinkx.api.serializer.*
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.IArray.genericWrapArray
import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

private[api] trait TypeInformationDerivation extends TaggedDerivation[TypeInformation]:

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  def cache: TrieMap[DerivationCacheKey, TypeInformation[?]] = TypeInformationDerivation.cache

  // We cannot add a constraint of `T <: Product`, even though `join` is always called on products.
  // Need to mix in via `& Product`.
  override def join[T](ctx: CaseClass[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val cacheKey = DerivationCacheKey(typeTag.toString, ctx.params.map(_.typeclass).toSeq)
    cache.get(cacheKey) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz = classTag.runtimeClass.asInstanceOf[Class[T & Product]]
        // An enum value is not versioned on its own: it's a member of its enum, whose version it is serialized with
        val annotations = if typeTag.isEnum then ctx.inheritedAnnotations else ctx.annotations
        val version     = Evolutions.findVersion(clazz, annotations.toSeq)
        val fieldNames  = ctx.parameters.map(_.label).toArray
        Evolutions.checkNoVersionOnFields(clazz, ctx.parameters.map(p => p.label -> p.annotations.toSeq).toSeq)

        val evolution = Evolutions.get(clazz, version)

        val serializer =
          if typeTag.isEnum then new Scala3EnumValueSerializer[T & Product](evolution, version, ctx.typeInfo.short)
          else if typeTag.isModule then new ScalaCaseObjectSerializer[T & Product](evolution, version)
          else
            new CaseClassSerializer[T & Product](
              evolution = evolution,
              version = version,
              isCaseClassImmutable = isCaseClassImmutable(clazz, fieldNames),
              fieldNames = fieldNames,
              paramSerializers = ctx.params.map { p =>
                val ser = p.typeclass.createSerializer(config)
                if (p.annotations.exists(_.isInstanceOf[nullable])) {
                  NullableSerializer.wrapIfNullIsNotSupported(ser, true)
                } else ser
              }.toArray
            )

        val ti = new CaseClassTypeInfo[T & Product](
          clazz = clazz,
          fieldTypes = ctx.params.map(_.typeclass),
          fieldNames = fieldNames,
          ser = serializer
        ).asInstanceOf[TypeInformation[T]]
        cache.putIfAbsent(cacheKey, ti).getOrElse(ti).asInstanceOf[TypeInformation[T]]

  override def split[T](ctx: SealedTrait[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val cacheKey = DerivationCacheKey(typeTag.toString, ctx.subtypes.map(_.typeclass).toSeq)
    cache.get(cacheKey) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz   = classTag.runtimeClass.asInstanceOf[Class[T]]
        val version = Evolutions.findVersion(clazz, ctx.annotations.toSeq)
        // An enum value is named by its enum, and has no class of its own to serialize
        val subtypeClasses: Array[Class[?]] =
          if typeTag.isEnum then Array.empty else ctx.subtypes.map(_.typeclass.getTypeClass).toArray[Class[?]]
        val memberNames: Array[String] =
          if typeTag.isEnum then ctx.subtypes.map(_.typeInfo.short).toArray else subtypeClasses.map(_.getName)

        val evolution = Evolutions.get(clazz, version)

        val serializer =
          if typeTag.isEnum then
            new Scala3EnumSerializer[T & Product](
              evolution = evolution.asInstanceOf[Evolution[T & Product]],
              version = version,
              enumValueNames = memberNames,
              enumValueSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
            ).asInstanceOf[TypeSerializer[T]]
          else
            new CoproductSerializer[T](
              evolution = evolution,
              version = version,
              subtypeClasses = subtypeClasses,
              subtypeFqns = memberNames,
              subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
            )

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        cache.putIfAbsent(cacheKey, ti).getOrElse(ti).asInstanceOf[TypeInformation[T]]

private[api] object TypeInformationDerivation:

  /** Storage of the cache exposed by [[TypeInformationDerivation.cache]]. */
  private val cache: TrieMap[DerivationCacheKey, TypeInformation[?]] = TrieMap.empty
