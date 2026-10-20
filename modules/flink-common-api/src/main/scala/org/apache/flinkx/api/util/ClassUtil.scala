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

  /** Resolve a former class binary name to its fully qualified binary name based on the current class.
    *
    * The binary name is the class name format where nested classes are separated by `$` instead of dots. See "Binary
    * names" section in [[java.lang.ClassLoader]] for more details or JLS 13.1 for the complete definition.
    *
    * The given `formerBinaryName` can be a relative or an absolute path.
    *
    * Any path referencing a package (dot-separated in binary name format) is an absolute path. Start with a `/` to
    * force absolute path (should be useful to reference unnamed package only).
    *
    * Other paths are resolved relatively to the parent of the given `currentClass` (i.e. next to `currentClass`). They
    * can contain `$` to reference nested classes.
    *
    * ==Examples==
    *
    * {{{
    * package org.example
    *
    * object Parent {
    *
    *   @version(1)
    *   @renamed(since = 1, formerName = "OldName")
    *   case object NewName
    * }
    * }}}
    *
    * With previous snippet, `formerName` can be:
    *   - `"OldName"` is resolved as relative to `org.example.Parent$OldName` former binary name
    *   - `"OldSubParent$OldName"` (relative) → `org.example.Parent$OldSubParent$OldName`
    *   - `"org.example.OldName"` (absolute) → `org.example.OldName`
    *   - `"old.example.OldName"` (absolute) → `old.example.OldName`
    *
    * @param formerBinaryName
    *   The former binary name
    * @param currentClass
    *   A class currently existing, used as context for resolving relative former binary name
    * @return
    *   The fully qualified internal former binary name (e.g., `org.example.Outer$Inner`)
    */
  def resolveFormerClassName(formerBinaryName: String, currentClass: Class[_]): String = {
    if (formerBinaryName == null || formerBinaryName.isEmpty) {
      throw new FlinkRuntimeException("Former binary name is mandatory")
    }
    val parentClass = currentClass.getDeclaringClass
    val packageName = currentClass.getPackageName
    if (formerBinaryName.head == '/') formerBinaryName.substring(1)
    else if (formerBinaryName.contains('.')) formerBinaryName
    else if (parentClass == null) { // The unnamed package qualifies no name
      if (packageName.isEmpty) formerBinaryName else s"$packageName.$formerBinaryName"
    } else {
      val parentClassName = parentClass.getName
      if (parentClassName.last == '$') s"$parentClassName$formerBinaryName" else s"$parentClassName$$$formerBinaryName"
    }
  }

  /** Load the class of the given name, or why it can't be loaded: not found, or found but failing to link or init. */
  def loadClass(className: String, initialize: Boolean, cl: ClassLoader): Try[Class[_]] =
    try Success(Class.forName(className, initialize, cl))
    catch {
      case error @ (_: ExceptionInInitializerError | _: LinkageError) =>
        Failure(new FlinkRuntimeException(s"Cannot load $className", error))
      case exception: Exception => Failure(exception)
    }

  /** Default value of the `index`-th field of the given case class, evaluated anew at each call.
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

  /** The binary name of the companion object of the given class name, an object being its own companion. */
  def companionName(className: String): String = if (className.endsWith("$")) className else s"$className$$"

  /** The companion object of the given class, or the object itself when the class is the one of an object.
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

  /** The instance of the given Scala object class, initializing it, none for another class. */
  def moduleOf(clazz: Class[_]): Option[AnyRef] = moduleField(clazz).map(_.get(null))

  private def moduleField(clazz: Class[_]): Option[Field] =
    clazz.getDeclaredFields.find(field => field.getName == "MODULE$" && Modifier.isStatic(field.getModifiers))

}
