package org.apache.flinkx.api.evolution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks the declaration a companion extending `Evolved` builds registers exactly what the annotations describe. */
class DeclarationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  import org.apache.flinkx.api.evolution.DeclarationTest._
  import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pet

  override protected def beforeEach(): Unit = Evolutions.reset()

  /** The windows registered for an ADT, as names, versions and current classes. */
  private def windowsOf[T](clazz: Class[T]): Map[String, Seq[(Int, String)]] =
    Evolutions
      .declaredEvolutions(clazz.getClassLoader)
      .map { case (name, evolutions) => name -> evolutions.toSeq.map(e => e.formerVersion -> e.currentClass.getName) }

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
    Evolutions.get(classOf[Probe], 2)

    windowsOf(classOf[Probe]).keySet shouldBe Set(
      classOf[Probe].getName,
      classOf[Probe].getName.replace("Probe", "FormerProbe"),
      classOf[Probe].getName.replace("Probe", "GoneType") // Declared deleted, so it resolves to the marker
    )
  }

  it should "declare a sealed trait and the former name it was renamed from" in {
    Evolutions.get(classOf[Pet], 2)

    windowsOf(classOf[Pet]).keySet should contain allOf (
      classOf[Pet].getName,
      classOf[Pet].getName.replace("Pet", "Animal")
    )
  }

  // The rules themselves: a rename, a transform with its mapper, a deletion and an addition with its default value
  it should "apply the same field evolutions as the derivation" in {
    val formerName   = classOf[Probe].getName.replace("Probe", "FormerProbe")
    val formerFields = Array("formerId", "count", "gone")

    Evolutions.get(classOf[Probe], 2)

    val failures = dryRunOf[Probe](formerName, 0, formerFields)
    withClue("the former name must resolve:")(failures shouldBe defined)
    // The transformed field loses its lineage by design, as does the added one
    withClue("every field of the current schema must be filled:")(
      failures shouldBe Some(Right(Seq(Some(0), None, None)))
    )
  }

  // The mappers live in the companion, hidden from the outside: the declaration is read from the companion itself
  it should "apply the mappers of the companion" in {
    val evolution = Evolutions.get(classOf[Probe], 2)

    evolution.postEvolve(1, Probe("id", "3", "default")) shouldBe Probe("id", "3", "default1")
  }

}

object DeclarationTest {

  /* Probe v0
  case class FormerProbe(formerId: String, count: Int, gone: String)
   */

  @version(2)
  @renamed(since = 1, "FormerProbe")
  @deletedFields(since = 1, "gone")
  @deletedClasses(since = 1, throwOnInstance = false, "GoneType")
  @postEvolution(Probe.bump)
  case class Probe(
      @renamed(since = 1, "formerId") id: String,
      @transformed(since = 1, Probe.intToString) count: String,
      @added(since = 2) label: String = "default"
  )

  object Probe extends Evolved[Probe] {
    // Visible from the annotations of the class, and from nowhere else
    private[DeclarationTest] def intToString(i: Int): String             = i.toString
    private[DeclarationTest] def bump(version: Int, probe: Probe): Probe = probe.copy(label = probe.label + version)
  }

}
