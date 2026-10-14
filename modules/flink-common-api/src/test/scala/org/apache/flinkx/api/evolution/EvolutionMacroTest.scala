package org.apache.flinkx.api.evolution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks the evolutions a companion extending `Evolving` builds register exactly what the annotations describe. */
class EvolutionMacroTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  import org.apache.flinkx.api.evolution.EvolutionMacroTest._
  import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pet

  override protected def beforeEach(): Unit = Evolutions.reset()

  /** The windows an ADT declares, as names, versions and current classes. */
  private def windowsOf(evolutions: Evolutions[_]): Map[String, Seq[(Int, String)]] =
    evolutions.byClassName.map { case (name, declared) =>
      name -> declared.toSeq.map(e => e.formerVersion -> e.currentClass.getName)
    }.toMap

  /** What the evolutions do to a former schema, which compares the rules themselves and the order they run in. */
  private def dryRunOf[T](
      formerName: String,
      formerVersion: Int,
      formerFields: Array[String]
  ): Option[Either[Seq[String], Seq[Option[Int]]]] =
    Option(Evolutions.get[T](formerName, formerVersion, getClass.getClassLoader))
      .map(_.dryRun(formerFields).left.map(_.map(_.getMessage).toSeq).map(_.toSeq))

  it should "declare the former and the current name of a case class" in {
    windowsOf(Probe.evolutions).keySet shouldBe Set(
      classOf[Probe].getName,
      classOf[Probe].getName.replace("Probe", "FormerProbe"),
      classOf[Probe].getName.replace("Probe", "GoneType") // Declared deleted, so it resolves to the marker
    )
  }

  it should "declare a sealed trait and the former name it was renamed from" in {
    windowsOf(Pet.evolutions).keySet should contain allOf (
      classOf[Pet].getName,
      classOf[Pet].getName.replace("Pet", "Animal")
    )
  }

  // The rules themselves: a rename, a transform with its mapper, a deletion and an addition with its default value
  it should "apply the same field evolutions as the derivation" in {
    val formerName   = classOf[Probe].getName.replace("Probe", "FormerProbe")
    val formerFields = Array("formerId", "count", "gone")

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

object EvolutionMacroTest {

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

  object Probe extends Evolving[Probe] {
    val evolutions = Evolutions[Probe]
    // Visible from the annotations of the class, and from nowhere else
    private[EvolutionMacroTest] def intToString(i: Int): String             = i.toString
    private[EvolutionMacroTest] def bump(version: Int, probe: Probe): Probe = probe.copy(label = probe.label + version)
  }

}
