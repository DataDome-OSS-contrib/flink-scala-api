package org.apache.flinkx.api.util

import org.apache.flink.util.FlinkRuntimeException

import java.lang.reflect.{Field, Modifier}

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
    val parentClass            = currentClass.getDeclaringClass
    val isCurrentClassTopLevel = parentClass == null
    val forceAbsolutePath      = formerBinaryName.head == '/'
    val absolutePath           = forceAbsolutePath || formerBinaryName.contains('.')
    if (absolutePath) {
      if (forceAbsolutePath) formerBinaryName.substring(1) else formerBinaryName
    } else if (isCurrentClassTopLevel) {
      s"${currentClass.getPackageName}.$formerBinaryName"
    } else {
      val parentClassName = parentClass.getName
      if (parentClassName.last == '$') s"$parentClassName$formerBinaryName" else s"$parentClassName$$$formerBinaryName"
    }
  }

}
