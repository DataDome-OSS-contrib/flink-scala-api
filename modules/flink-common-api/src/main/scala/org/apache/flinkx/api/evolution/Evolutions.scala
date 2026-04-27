package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.util.ClassUtil
import org.apache.flinkx.api.version

import scala.annotation.StaticAnnotation
import scala.collection.concurrent

object Evolutions {

  // Maps filled at startup time during the ADT derivation with up-to-date information from the source code and used
  // during deserialization
  private val classToEvolutions: concurrent.Map[Class[_], Evolution[_]]       = concurrent.TrieMap.empty
  private val formerClassNameToCurrentClass: concurrent.Map[String, Class[_]] = concurrent.TrieMap.empty

  def findVersion[A](clazz: Class[_]): PartialFunction[A, Int] = {
    case version(c) if c > 0 => c
    case version(c) => throw new FlinkRuntimeException(s"Current version of $clazz must be positive, got @version($c)")
  }

  /** Return the [[Evolution]] associated with given class. Create it if necessary.
    *
    * @param clazz
    *   ADT class to deserialize and evolve
    */
  def get[T](clazz: Class[T]): Evolution[T] =
    classToEvolutions.getOrElseUpdate(clazz, new Evolution[T](clazz)).asInstanceOf[Evolution[T]]

  /** Register the mapping between the former ADT class name and the ADT class currently declared on the source code.
    *
    * @param formerClassName
    *   The fully qualified name of the ADT class as it was declared when it has been serialized
    * @param currentClass
    *   ADT class being derived
    */
  def registerFormerClass(formerClassName: String, currentClass: Class[_]): Unit =
    formerClassNameToCurrentClass(ClassUtil.resolveFormerClassName(formerClassName, currentClass)) = currentClass

  /** Register that the former ADT subtype has been deleted in the current source code.
    *
    * @param formerSubtypeName
    *   The fully qualified name of the ADT subtype as it was declared when it has been serialized
    * @param currentClass
    *   ADT class being derived
    */
  def registerDeletedFormerClass(formerSubtypeName: String, currentClass: Class[_]): Unit =
    formerClassNameToCurrentClass(ClassUtil.resolveFormerClassName(formerSubtypeName, currentClass)) = DeletedClass

  /** Resolve the current ADT class based on the former ADT class name.
    *
    * @param formerClassName
    *   The fully qualified name of the ADT class as it was declared when it has been serialized
    * @param cl
    *   The user code class loader
    * @return
    *   The current ADT class
    */
  def resolveFormerClass[T](formerClassName: String, cl: ClassLoader): Class[T] =
    formerClassNameToCurrentClass
      .getOrElse(formerClassName, ClassUtil.resolveClassByName(formerClassName, cl))
      .asInstanceOf[Class[T]]

  private[api] def throwEvolutionNotAllowed[A](evolution: StaticAnnotation, target: String): A =
    throw new FlinkRuntimeException(s"@$evolution annotation is not allowed on $target")

  @VisibleForTesting
  private[api] def reset(): Unit = {
    classToEvolutions.clear()
    formerClassNameToCurrentClass.clear()
  }

}
