package org.apache.flinkx.api.evolution

import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassReader
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, IOException}
import java.net.{URL, URLClassLoader}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import java.util.jar.{Attributes, JarFile}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Try, Using}

/** Companion scan: finds the evolving companions in the jars and class directories of a class loader.
  *
  * The evolving companions of a job are in the jars of its class loader: the scan does not read the jars of the parent
  * class loaders, such as the `lib` directory of the cluster. Flink gives a wrapper of this class loader, with the URLs
  * of the class loader that it wraps. Without URLs, the scan reads the class path of the JVM: a job in a MiniCluster,
  * or a test, has its classes there. Like the JVM, it also reads the jars that the `Class-Path` of a manifest lists,
  * and skips an entry that it cannot read: a file that is not a jar, a truncated jar, an unreadable directory.
  *
  * A companion can extend [[Evolving]] through a trait or a class of its own. The scan reads only the header of each
  * class file, then its supertypes. It reads each supertype once, and skips the JDK and the Scala library.
  */
private[evolution] object CompanionScan {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val EvolvingName = classOf[Evolving[_]].getName.replace('.', '/')

  /** Packages that contain no evolving companion: the scan reads none of their class files. */
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
    val roots =
      if (urls.nonEmpty) urls
      else
        System.getProperty("java.class.path", "").split(File.pathSeparator).toSeq.filter(_.nonEmpty).map(Paths.get(_))
    withManifestClassPaths(roots)
  }

  /** The given roots, and the jars that the `Class-Path` of their manifests lists, as the JVM reads them. */
  private def withManifestClassPaths(roots: Seq[Path]): Seq[Path] = {
    val all                   = mutable.LinkedHashSet.empty[Path]
    def add(root: Path): Unit = if (all.add(root.toAbsolutePath.normalize)) manifestClassPath(root).foreach(add)
    roots.foreach(add)
    all.toSeq
  }

  /** The roots that the `Class-Path` of the manifest of the given jar lists, as URLs relative to the jar. */
  private def manifestClassPath(root: Path): Seq[Path] =
    if (!Files.isRegularFile(root)) Nil
    else
      try
        Using.resource(new JarFile(root.toFile)) { jar =>
          Option(jar.getManifest)
            .flatMap(manifest => Option(manifest.getMainAttributes.getValue(Attributes.Name.CLASS_PATH)))
            .toSeq
            .flatMap(_.split(' ').filter(_.nonEmpty))
            .flatMap(entry => Try(Paths.get(root.toUri.resolve(entry))).toOption) // The JVM ignores an invalid URL
        }
      catch { case NonFatal(_) => Nil } // companionsIn skips this root, and logs why

  /** The path of a `file` URL, or none if unreadable. Flink gives the jars of `usrlib` as unencoded relative URLs. */
  private def pathOf(url: URL): Option[Path] =
    if (url.getProtocol == "file")
      try Some(Try(Paths.get(url.toURI)).getOrElse(Paths.get(url.getPath))) // Read as the URLClassLoader reads it
      catch { case NonFatal(failure) => skipped(url, failure) }
    else None

  private def companionsIn(root: Path, supertypes: Supertypes): Seq[String] =
    try
      if (Files.isDirectory(root)) companionsInDirectory(root, supertypes)
      else if (Files.isRegularFile(root)) companionsInJar(root, supertypes)
      else Seq.empty
    catch { case NonFatal(failure) => skipped(root, failure).toList }

  /** The companions of the class directory `root`. An unreadable file or subdirectory does not stop the scan. */
  private def companionsInDirectory(root: Path, supertypes: Supertypes): Seq[String] = {
    val companions = mutable.ArrayBuffer.empty[String]
    Files.walkFileTree(
      root,
      new SimpleFileVisitor[Path] {
        override def visitFile(path: Path, attributes: BasicFileAttributes): FileVisitResult = {
          if (attributes.isRegularFile && isCompanionFile(entryName(root, path)))
            try companions ++= companionNamed(Files.readAllBytes(path), supertypes)
            catch { case failure: IOException => skipped(path, failure) }
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(path: Path, failure: IOException): FileVisitResult = {
          skipped(path, failure)
          FileVisitResult.CONTINUE
        }
      }
    )
    companions.toList
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

  /** Log why the scan skips an entry that it cannot read, like the JVM. */
  private def skipped(entry: Any, failure: Throwable): Option[Nothing] = {
    log.warn(s"The companion scan skips $entry: $failure")
    None
  }

  /** The name of a class file in a class directory, as in a jar. */
  private def entryName(root: Path, path: Path): String =
    root.relativize(path).toString.replace(File.separatorChar, '/')

  /** The class of a companion object has the name of its type, with a final `$`. */
  private def isCompanionFile(name: String): Boolean =
    name.endsWith("$.class") && !LibraryPrefixes.exists(name.startsWith)

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
