package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolutions
import org.apache.flinkx.api.evolution.Evolutions.throwEvolutionNotAllowed
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer, nullable}
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation, MarkerTypeInfo}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.collection.mutable
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.{ClassTag, classTag}

private[api] trait TypeInformationDerivation {

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  protected val cache: mutable.Map[String, TypeInformation[_]] = mutable.Map.empty

  def join[T <: Product: ClassTag: TypeTag](
      ctx: CaseClass[TypeInformation, T]
  ): TypeInformation[T] = {
    val cacheKey = typeName[T]
    cache.get(cacheKey) match {
      case Some(MarkerTypeInfo) =>
        throw new FlinkRuntimeException(s"Unsupported: recursivity detected in '$cacheKey'.")
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        cache.put(cacheKey, MarkerTypeInfo)
        val clazz      = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val version    = ctx.annotations.collectFirst { case v: version => v.current }.getOrElse(0)
        val fieldNames = ctx.parameters.map(_.label).toArray
        val serializer = if (typeOf[T].typeSymbol.isModuleClass) {
          new ScalaCaseObjectSerializer[T](clazz)
        } else {
          new CaseClassSerializer[T](
            clazz = clazz,
            isCaseClassImmutable = isCaseClassImmutable(clazz, fieldNames),
            version = version,
            fieldNames = fieldNames,
            paramSerializers = ctx.parameters.map { p =>
              val ser = p.typeclass.createSerializer(config)
              if (p.annotations.exists(_.isInstanceOf[nullable])) {
                NullableSerializer.wrapIfNullIsNotSupported(ser, true)
              } else ser
            }.toArray
          )
        }

        val evolution = Evolutions.get(clazz)
        evolution.addFieldNames(fieldNames) // Need field names even with version 0
        if (version == 0) {
          // Do not allow Evolution annotations without a version
          ctx.annotations.collect {
            case _: renamed if ctx.inheritedAnnotations.exists(_.isInstanceOf[version]) => // Allow @renamed if parent has version
            case e: Evolved => throwEvolutionNotAllowed(e, s"$clazz without version")
          }
          ctx.parameters.foreach { p =>
            p.annotations.collect { case e: Evolved => throwEvolutionNotAllowed(e, s"$p of $clazz without version") }
          }
        } else { // version > 0
          // Iterate over case class annotations to register evolutions from current source code
          ctx.annotations.collect {
            case r: renamed            => Evolutions.registerFormerClass(r.from, clazz)
            case d: deletedMembers     => d.names.map(Delete(d.since, clazz, _)).foreach(evolution.addFieldEvolution)
            case p: postDeserialize[T] => evolution.addPostDeserialize(p.mapper)
            case e: Evolved            => throwEvolutionNotAllowed(e, clazz.toString)
          }
          // Iterate over case class fields annotations to register evolutions from current source code
          ctx.parameters.foreach { p =>
            p.annotations.collect {
              case a: added             => evolution.addFieldEvolution(Add(a.since, clazz, p.label, p.default))
              case r: renamed           => evolution.addFieldEvolution(Rename(r.since, clazz, p.label, r.from))
              case t: transformed[_, _] => evolution.addFieldEvolution(Transform(t.since, clazz, p.label, t.mapper))
              case e: version           => throwEvolutionNotAllowed(e, s"$clazz.${p.label}")
              case e: Evolved           => throwEvolutionNotAllowed(e, s"$clazz.${p.label}")
            }
          }
        }

        val ti = new CaseClassTypeInfo[T](
          clazz = clazz,
          fieldTypes = ctx.parameters.map(_.typeclass),
          fieldNames = fieldNames,
          ser = serializer
        )
        cache.put(cacheKey, ti)
        ti
    }
  }

  def split[T: ClassTag: TypeTag](ctx: SealedTrait[TypeInformation, T]): TypeInformation[T] = {
    val cacheKey = typeName[T]
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T]]
        val version    = ctx.annotations.collectFirst { case v: version => v.current }.getOrElse(0)
        val serializer = new CoproductSerializer[T](
          clazz = clazz,
          subtypeClasses = ctx.subtypes.map(_.typeclass.getTypeClass).toArray,
          subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
        )

        if (version == 0) {
          // Do not allow Evolution annotations without a version
          ctx.annotations.collect { case e: Evolved => throwEvolutionNotAllowed(e, s"$clazz without version") }
          ctx.subtypes.foreach { p =>
            p.annotations.collect { case e: Evolved => throwEvolutionNotAllowed(e, s"$p of $clazz without version") }
          }
        } else { // version > 0
          val evolution = Evolutions.get(clazz)
          // Iterate over coproduct annotations to register evolutions from current source code
          ctx.annotations.collect {
            case r: renamed            => Evolutions.registerFormerClass(r.from, clazz)
            case d: deletedMembers     => d.names.foreach(Evolutions.registerDeletedFormerClass(_, clazz))
            case p: postDeserialize[T] => evolution.addPostDeserialize(p.mapper)
            case e: Evolved            => throwEvolutionNotAllowed(e, clazz.toString)
          }
          // Iterate over subtypes annotations to register evolutions from current source code
          ctx.subtypes.foreach { p =>
            p.annotations.collect {
              case r: renamed        => Evolutions.registerFormerClass(r.from, p.typeclass.getTypeClass)
              case _: deletedMembers => // allowed on subtype
              case e: Evolved        => throwEvolutionNotAllowed(e, p.typeclass.getTypeClass.toString)
            }
          }
        }

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        cache.put(cacheKey, ti)
        ti
    }
  }

  private def typeName[T: TypeTag]: String = typeOf[T].toString

}
