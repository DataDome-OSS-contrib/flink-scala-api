package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.{added, deletedClasses, deletedFields, postDeserialize, renamed, transformed, version}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks `Declare.declare[T]` registers exactly what the derivation registers, without deriving anything. */
class DeclareTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  import org.apache.flinkx.api.evolution.DeclareTest._
  import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pet

  override protected def beforeEach(): Unit = Evolutions.reset()

  /** The windows registered for an ADT, as names, versions and current classes. */
  private def windowsOf[T](clazz: Class[T]): Map[String, Seq[(Int, String)]] =
    Evolutions
      .declaredEvolutions(clazz.getClassLoader)
      .map { case (name, evolutions) => name -> evolutions.toSeq.map(e => e.version -> e.currentClass.getName) }

  /** What the evolutions do to a former schema, which compares the rules themselves and the order they run in. */
  private def dryRunOf[T](
      formerName: String,
      formerVersion: Int,
      formerFields: Array[String]
  ): Option[Either[Seq[String], Seq[Option[Int]]]] =
    Evolutions
      .find[T](formerName, formerVersion, getClass.getClassLoader)
      .map(_.dryRun(formerFields).left.map(_.map(_.getMessage).toSeq).map(_.toSeq))

  it should "declare the former and the current name of a case class" in {
    Declare.declare[Probe]

    windowsOf(classOf[Probe]).keySet shouldBe Set(
      classOf[Probe].getName,
      classOf[Probe].getName.replace("Probe", "FormerProbe"),
      classOf[Probe].getName.replace("Probe", "GoneType") // Declared deleted, so it resolves to the marker
    )
  }

  it should "declare a sealed trait and the former name it was renamed from" in {
    Declare.declare[Pet]

    windowsOf(classOf[Pet]).keySet shouldBe Set(
      classOf[Pet].getName,
      classOf[Pet].getName.replace("Pet", "Animal")
    )
  }

  // The rules themselves: a rename, a transform with its mapper, a deletion and an addition with its default value
  it should "apply the same field evolutions as the derivation" in {
    val formerName   = classOf[Probe].getName.replace("Probe", "FormerProbe")
    val formerFields = Array("formerId", "count", "gone")

    Declare.declare[Probe]

    val failures = dryRunOf[Probe](formerName, 0, formerFields)
    withClue("the former name must resolve:")(failures shouldBe defined)
    // The transformed field loses its lineage by design, as does the added one
    withClue("every field of the current schema must be filled:")(
      failures shouldBe Some(Right(Seq(Some(0), None, None)))
    )
  }

}

object DeclareTest {

  /* Probe v0
  case class FormerProbe(formerId: String, count: Int, gone: String)
   */

  @version(2)
  @renamed(since = 1, "FormerProbe")
  @deletedFields(since = 1, "gone")
  @deletedClasses(since = 1, throwOnInstance = false, "GoneType")
  @postDeserialize(bump)
  case class Probe(
      @renamed(since = 1, "formerId") id: String,
      @transformed(since = 1, intToString) count: String,
      @added(since = 2) label: String = "default"
  )

  def intToString(i: Int): String             = i.toString
  def bump(version: Int, probe: Probe): Probe = probe.copy(label = probe.label + version)
}
