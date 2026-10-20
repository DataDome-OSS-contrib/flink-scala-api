package org.apache.flinkx.api.evolution

import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassReader

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths}
import java.util.jar.JarFile
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Companion scan: finds the evolving companions in the jars and class directories of a class loader.
  *
  * The evolving companions of a job are all in the jars of its class loader: the parent class loaders cannot see
  * [[Evolving]]. Flink gives a wrapper of this class loader, with the URLs of the class loader that it wraps. Without
  * URLs, the scan reads the class path of the JVM: a job in a MiniCluster, or a test, has its classes there.
  *
  * A companion can extend [[Evolving]] through a trait or a class of its own. The scan reads only the header of each
  * class file, then its supertypes. It reads each supertype once, and skips the JDK and the Scala library.
  */
private[evolution] object CompanionScan {

  private val EvolvingName = classOf[Evolving[_]].getName.replace('.', '/')

  /** Packages that contain no evolving companion: the scan does not read their types */
  private val LibraryPrefixes = Seq("java/", "javax/", "jdk/", "scala/")

  /** Class names of the evolving companions in the jars and class directories of the given class loader. */
  def companionsOf(classLoader: ClassLoader): Seq[String] = {
    val supertypes = new Supertypes(classLoader)
    rootsOf(classLoader).flatMap(companionsIn(_, supertypes)).distinct
  }

  private def rootsOf(classLoader: ClassLoader): Seq[Path] = {
    val urls = classLoader match {
      case loader: URLClassLoader => loader.getURLs.toSeq.flatMap(pathOf)
      case _                      => Seq.empty
    }
    if (urls.nonEmpty) urls
    else System.getProperty("java.class.path", "").split(File.pathSeparator).toSeq.filter(_.nonEmpty).map(Paths.get(_))
  }

  private def pathOf(url: URL): Option[Path] =
    if (url.getProtocol == "file") Some(Paths.get(url.toURI)) else None

  private def companionsIn(root: Path, supertypes: Supertypes): Seq[String] =
    if (Files.isDirectory(root)) companionsInDirectory(root, supertypes)
    else if (Files.isRegularFile(root)) companionsInJar(root, supertypes)
    else Seq.empty

  private def companionsInDirectory(root: Path, supertypes: Supertypes): Seq[String] =
    Using.resource(Files.walk(root)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(path => isCompanionFile(path.toString) && Files.isRegularFile(path))
        .flatMap(path => companionNamed(Files.readAllBytes(path), supertypes))
        .toList
    }

  private def companionsInJar(jar: Path, supertypes: Supertypes): Seq[String] =
    Using.resource(new JarFile(jar.toFile)) { file =>
      file
        .entries()
        .asScala
        .filter(entry => !entry.isDirectory && isCompanionFile(entry.getName))
        .flatMap(entry => companionNamed(Using.resource(file.getInputStream(entry))(_.readAllBytes()), supertypes))
        .toList
    }

  /** The class of a companion object has the name of its type, with a final `$`. */
  private def isCompanionFile(name: String): Boolean = name.endsWith("$.class")

  /** The class name of the given class file, if it extends [[Evolving]]. */
  private def companionNamed(classFile: Array[Byte], supertypes: Supertypes): Option[String] =
    headerOf(classFile).filter(supertypes.extendsEvolving).map(_.getClassName.replace('/', '.'))

  /** The header of the given class file, or none if ASM cannot read its class file version. */
  private def headerOf(classFile: Array[Byte]): Option[ClassReader] =
    try Some(new ClassReader(classFile))
    catch { case _: IllegalArgumentException => None }

  /** Tells if a class extends [[Evolving]] through its supertypes. A scan reads each supertype once. */
  private final class Supertypes(classLoader: ClassLoader) {

    private val known = mutable.Map.empty[String, Boolean]

    def extendsEvolving(header: ClassReader): Boolean =
      (Option(header.getSuperName) ++ header.getInterfaces).exists(isEvolving)

    private def isEvolving(name: String): Boolean =
      name == EvolvingName || !LibraryPrefixes.exists(name.startsWith) && known.getOrElse(name, resolve(name))

    private def resolve(name: String): Boolean = {
      known(name) = false // Stops the walk on a cycle, which an inconsistent class path can contain
      val extending = classFile(name).flatMap(headerOf).exists(extendsEvolving)
      known(name) = extending
      extending
    }

    private def classFile(name: String): Option[Array[Byte]] =
      Option(classLoader.getResourceAsStream(s"$name.class")).map(in => Using.resource(in)(_.readAllBytes()))

  }

}
