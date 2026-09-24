package org.apache.flinkx.api.serializer

import scala.annotation.nowarn
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe
import scala.util.control.NonFatal

private[serializer] trait ConstructorCompat {

  @nowarn("msg=explicit array")
  final def lookupConstructor[T <: Product](cls: Class[T]): Array[AnyRef] => T = {
    val rootMirror  = universe.runtimeMirror(cls.getClassLoader)
    val classSymbol = rootMirror.classSymbol(cls)

    require(
      classSymbol.isStatic,
      s"""
         |The class ${cls.getSimpleName} is an instance class, meaning it is not a member of a
         |top level object, or of an object contained in a top level object,
         |therefore it requires an outer instance to be instantiated, but we don't have a
         |reference to the outer instance. Please consider changing the outer class to an object.
         |""".stripMargin
    )

    val classMirror = rootMirror.reflectClass(classSymbol)
    val constructor = classMirror.reflectConstructor(primaryConstructorOf(classSymbol))

    (args: Array[AnyRef]) => constructor.apply(args: _*).asInstanceOf[T]
  }

  /** Names of the parameters [[lookupConstructor]] instantiates the given class with, in their order, empty when they
    * can't be read.
    */
  final def lookupFieldNames(cls: Class[_]): Array[String] =
    try {
      val classSymbol = universe.runtimeMirror(cls.getClassLoader).classSymbol(cls)
      primaryConstructorOf(classSymbol).paramLists.flatten.map(_.name.decodedName.toString).toArray
    } catch {
      case NonFatal(_) => Array.empty
    }

  @nowarn("msg=eliminated by erasure")
  private def primaryConstructorOf(classSymbol: ClassSymbol): MethodSymbol =
    classSymbol.toType
      .decl(universe.termNames.CONSTRUCTOR)
      .alternatives
      .collectFirst {
        case constructorSymbol: universe.MethodSymbol if constructorSymbol.isPrimaryConstructor =>
          constructorSymbol
      }
      .head
      .asMethod

}

/** Reads the constructor of a case class outside of its serializer, from its snapshot in particular. */
private[api] object ConstructorCompat extends ConstructorCompat
