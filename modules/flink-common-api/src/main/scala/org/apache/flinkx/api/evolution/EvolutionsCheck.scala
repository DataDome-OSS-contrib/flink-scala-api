package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.PublicEvolving

import java.io.File
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Checks the versioned ADTs of a module are all declared, from a test of the module declaring them.
  *
  * An ADT no [[EvolutionsProvider]] declares restores as if it had never evolved, which the restore reports far from
  * the source code that forgot it: this reports it while building instead.
  */
@PublicEvolving
object EvolutionsCheck {

  private val PackageClause  = """^\s*package\s+([\w.]+)\s*\{?\s*$""".r
  private val VersionClause  = """^\s*@version\s*\(\s*(?:current\s*=\s*)?(\d+)\s*\).*$""".r
  private val AnnotationLine = """^\s*@.*$""".r
  private val TypeClause     =
    """^\s*(?:(?:final|sealed|abstract|implicit|private|protected|open)\s+)*(?:case\s+)?(?:class|trait|object|enum)\s+(\w+).*$""".r

  /** Names of the `@version` annotated ADTs of the given package, sub-packages included, that no [[EvolutionsProvider]]
    * declares.
    *
    * Reads the sources rather than the compiled classes, so that an ADT added since the provider was last compiled is
    * reported: an incremental build recompiles neither the provider nor this check when a source file is added.
    *
    * @param sourceDirectory
    *   Root of the sources to read, typically `new File("src/main/scala")`
    * @param packageName
    *   Package the ADTs to check are declared in
    * @param classLoader
    *   Class loader the declarations are looked up for
    */
  def undeclaredIn(
      sourceDirectory: File,
      packageName: String,
      classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
  ): Seq[String] = {
    val declared = Evolutions.declaredNames(classLoader)
    versionedIn(sourceDirectory.toPath, packageName).collect {
      case (adtPackage, name) if !declared.exists(isNamed(_, adtPackage, name)) => s"$adtPackage.$name"
    }
  }

  /** A declared name ends with the ADT name, whatever the objects enclosing it, and starts with its package. */
  private def isNamed(declaredName: String, adtPackage: String, name: String): Boolean =
    declaredName.startsWith(s"$adtPackage.") && declaredName.split("[.$]").lastOption.contains(name)

  /** Package and name of every `@version` annotated ADT declared under the given source directory. */
  private def versionedIn(sourceDirectory: Path, packageName: String): Seq[(String, String)] =
    if (!Files.isDirectory(sourceDirectory)) Seq.empty
    else
      Using.resource(Files.walk(sourceDirectory)) { paths =>
        paths
          .iterator()
          .asScala
          .filter(path => path.toString.endsWith(".scala"))
          .flatMap(versionedInFile)
          .filter { case (adtPackage, _) => adtPackage == packageName || adtPackage.startsWith(s"$packageName.") }
          .toSeq
      }

  private def versionedInFile(source: Path): Seq[(String, String)] =
    Using.resource(Files.lines(source)) { lines =>
      var adtPackage = ""
      var versioned  = false
      lines
        .iterator()
        .asScala
        .flatMap {
          case PackageClause(declared) if !versioned =>
            adtPackage = if (adtPackage.isEmpty) declared else s"$adtPackage.$declared"
            None
          case VersionClause(_)                  => versioned = true; None
          case AnnotationLine() if versioned     => None // Other annotations of the same ADT
          case TypeClause(name) if versioned     => versioned = false; Some(adtPackage -> name)
          case line if versioned && line.isBlank => None
          case _                                 => versioned = false; None
        }
        .toSeq
    }

}
