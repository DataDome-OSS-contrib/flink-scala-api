package org.apache.flinkx.api.util

import org.apache.flink.util.FlinkRuntimeException

import java.lang.reflect.{Field, InvocationTargetException, Modifier}
import scala.util.{Failure, Success, Try}

object ClassUtil {

  /** Checks if given case class is immutable by checking if all its parameters are val (final fields).
    *
    * @param clazz
    *   The case class to check.
    * @param paramNames
    *   The names of the case class parameters.
    * @return
    *   true if all the case class parameters are immutable, false otherwise.
    */
  def isCaseClassImmutable(clazz: Class[_], paramNames: Array[String]): Boolean = {
    val declaredFields: Array[Field] = clazz.getDeclaredFields
    paramNames.forall(paramName =>
      declaredFields
        .find(field => field.getName == paramName)
        .orElse(declaredFields.find(field => field.getName == s"${clazz.getName.replace('.', '$')}$$$$$paramName"))
        // return true if the field isn't found in the class: case classes can only override val from parent classes
        .forall(field => Modifier.isFinal(field.getModifiers))
    )
  }

  /** Resolves the former class name based on the given current class.
    *
    * A class name is a binary name: a `$` separates a nested class from its enclosing class (see "Binary names" in
    * [[java.lang.ClassLoader]], or JLS 13.1). The former class name is:
    *   - absolute if it contains a package (with dots), or if it starts with `/`, for a class of the unnamed package;
    *   - relative otherwise: it is resolved next to `currentClass`, and it can contain `$` for a nested class.
    *
    * ==Examples==
    * {{{
    * package org.example
    *
    * object Parent {
    *   @version(1)
    *   @renamed(since = 1, "FormerName")
    *   case object CurrentName
    * }
    * }}}
    *   - `"FormerName"` → `org.example.Parent$FormerName`
    *   - `"FormerParent$FormerName"` → `org.example.Parent$FormerParent$FormerName`
    *   - `"org.example.FormerName"` → `org.example.FormerName`
    *   - `"/FormerName"` → `FormerName`, in the unnamed package
    *
    * @param formerBinaryName
    *   The former class name that the annotation gives
    * @param currentClass
    *   The annotated class, from which a relative name is resolved
    * @return
    *   The former class name, for example `org.example.Outer$Inner`
    */
  def resolveFormerClassName(formerBinaryName: String, currentClass: Class[_]): String = {
    if (formerBinaryName == null || formerBinaryName.isEmpty) {
      throw new FlinkRuntimeException("The former class name is empty")
    }
    val parentClass = currentClass.getDeclaringClass
    val packageName = currentClass.getPackageName
    if (formerBinaryName.head == '/') formerBinaryName.substring(1)
    else if (formerBinaryName.contains('.')) formerBinaryName
    else if (parentClass == null) { // A class of the unnamed package has no package in its name
      if (packageName.isEmpty) formerBinaryName else s"$packageName.$formerBinaryName"
    } else {
      val parentClassName = parentClass.getName
      if (parentClassName.last == '$') s"$parentClassName$formerBinaryName" else s"$parentClassName$$$formerBinaryName"
    }
  }

  /** Loads the class with the given name, or gives why it does not load: not found, link or initialization error. */
  def loadClass(className: String, initialize: Boolean, cl: ClassLoader): Try[Class[_]] =
    try Success(Class.forName(className, initialize, cl))
    catch {
      case error @ (_: ExceptionInInitializerError | _: LinkageError) =>
        Failure(new FlinkRuntimeException(s"Cannot load $className", error))
      case exception: Exception => Failure(exception)
    }

  /** Default value of the field at `index` in the given case class, which each call of the function evaluates again.
    *
    * @throws java.lang.NoSuchMethodException
    *   if the field declares no default value
    */
  def defaultFieldValue(caseClass: Class[_], index: Int): () => AnyRef = {
    val companion = companionInstance[AnyRef](caseClass)
    val method    = companion.getClass.getMethod(s"$$lessinit$$greater$$default$$${index + 1}")
    () =>
      try method.invoke(companion)
      catch { case e: InvocationTargetException => throw e.getCause }
  }

  /** Class name of the companion object of the given class name. An object is its own companion. */
  def companionName(className: String): String = if (className.endsWith("$")) className else s"$className$$"

  /** The companion object of the given class. For the class of an object, the object itself.
    * @throws java.lang.ClassNotFoundException
    *   if the class has no companion object
    */
  def companionInstance[A](clazz: Class[_]): A = {
    val companion = Class.forName(companionName(clazz.getName), true, clazz.getClassLoader)
    moduleOf(companion)
      .getOrElse(throw new ClassNotFoundException(s"${companion.getName} is not an object"))
      .asInstanceOf[A]
  }

  /** Whether the given class is the class of a Scala object, which holds its instance in a static `MODULE$` field. */
  def isObjectClass(clazz: Class[_]): Boolean = moduleField(clazz).isDefined

  /** Whether the given class is the mirror class of a top-level Scala object: it holds the static forwarders of the
    * object, under its name without `$`, and has no constructor.
    */
  def isMirrorClass(clazz: Class[_]): Boolean = !clazz.isInterface && clazz.getDeclaredConstructors.isEmpty

  /** The instance of the given Scala object class, which this call initializes. None for another class. */
  def moduleOf(clazz: Class[_]): Option[AnyRef] = moduleField(clazz).map(_.get(null))

  private def moduleField(clazz: Class[_]): Option[Field] =
    clazz.getDeclaredFields.find(field => field.getName == "MODULE$" && Modifier.isStatic(field.getModifiers))

}
