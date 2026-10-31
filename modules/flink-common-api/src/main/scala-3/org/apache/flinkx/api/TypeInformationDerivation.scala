package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.evolution.AdtEvolution.NoEvolution
import org.apache.flinkx.api.evolution.{AdtEvolution, Evolvable}
import org.apache.flinkx.api.serializer.*
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.IArray.genericWrapArray
import scala.reflect.ClassTag

private[api] trait TypeInformationDerivation extends TaggedDerivation[TypeInformation]:

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  def cache: DerivationCache = TypeInformationDerivation.cache

  // We cannot add a constraint of `T <: Product`, even though `join` is always called on products.
  // Need to mix in via `& Product`.
  override def join[T](ctx: CaseClass[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T],
      evolvable: Evolvable[T]
  ): Typeclass[T] =
    val cacheKey = DerivationCacheKey(classTag.runtimeClass, typeTag.toString, ctx.params.map(_.typeclass).toSeq)
    cache.get(cacheKey) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T & Product]]
        val fieldNames = ctx.parameters.map(_.label).toArray

        val evolution = new NoEvolution(clazz, evolvable.version, fieldNames)

        val serializer =
          if typeTag.isEnum then new Scala3EnumValueSerializer[T & Product](evolution, ctx.typeInfo.short)
          else if typeTag.isModule then new ScalaCaseObjectSerializer[T & Product](evolution)
          else
            new CaseClassSerializer[T & Product](
              evolution = evolution,
              isCaseClassImmutable = isCaseClassImmutable(clazz, fieldNames),
              fieldNames = Some(fieldNames),
              scalaFieldSerializers = ctx.params.map { p =>
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
      typeTag: TypeTag[T],
      evolvable: Evolvable[T]
  ): Typeclass[T] =
    val cacheKey = DerivationCacheKey(classTag.runtimeClass, typeTag.toString, ctx.subtypes.map(_.typeclass).toSeq)
    cache.get(cacheKey) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz             = classTag.runtimeClass.asInstanceOf[Class[T]]
        val evolution         = new NoEvolution(clazz, evolvable.version)
        val memberSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray[TypeSerializer[?]]

        val serializer =
          // An enum value has no class of its own: the enum serializer refers to its values by name
          if typeTag.isEnum then
            new Scala3EnumSerializer[T & Product](
              evolution = Some(evolution.asInstanceOf[AdtEvolution[T & Product]]),
              enumValueNames = ctx.subtypes.map(_.typeInfo.short).toArray,
              enumValueSerializers = memberSerializers
            ).asInstanceOf[TypeSerializer[T]]
          else
            val subtypeClasses = ctx.subtypes.map(_.typeclass.getTypeClass).toArray[Class[?]]
            new CoproductSerializer[T](
              evolution = Some(evolution),
              subtypeClasses = subtypeClasses,
              subtypeNames = subtypeClasses.map(_.getName),
              subtypeSerializers = memberSerializers
            )

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        cache.putIfAbsent(cacheKey, ti).getOrElse(ti).asInstanceOf[TypeInformation[T]]

private[api] object TypeInformationDerivation:

  /** Storage of the cache exposed by [[TypeInformationDerivation.cache]]. */
  private val cache: DerivationCache = new DerivationCache()
