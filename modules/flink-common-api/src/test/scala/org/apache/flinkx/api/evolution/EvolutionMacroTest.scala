package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.evolution.EvolutionTest.AddedFieldWithoutAnnotation
import org.apache.flinkx.api.evolution.FieldEvolution.Former
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** What `Evolutions[T]` builds from the annotations of an ADT: its declarations, and the ADT evolution of each version.
  * An ADT evolution plans the migration of the former fields, or tells why it cannot.
  */
class EvolutionMacroTest extends AnyFlatSpec with Matchers {

  import org.apache.flinkx.api.evolution.EvolutionMacroTest._
  import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pet

  /** The class names an ADT declares. */
  private def namesOf(evolutions: Evolutions[_]): Set[String] = evolutions.declarations.map(_.className).toSet

  /** For each current field, the index of the former field that it reads, or none for an added or transformed field.
    * The result shows how the field evolutions change a former schema, and in which order they apply.
    */
  private def lineageOf(
      evolution: AdtEvolution[_],
      formerFields: Array[String]
  ): Either[Seq[String], Seq[Option[Int]]] =
    evolution
      .plan(formerFields)
      .map(_.toSeq.map {
        case Former(index) => Some(index)
        case _             => None
      })

  // The field names of Click at version 0, as its snapshot records them
  private val ClickV0FieldNames = Array("a", "inFileClicks", "fieldNotInFile", "identifier", "b")

  /** All the failures that the plan of `clazz` reports for the fields of Click at version 0. */
  private def planFailures(clazz: Class[_]): Seq[String] =
    evolutionOf(clazz).plan(ClickV0FieldNames).swap.getOrElse(Seq.empty)

  it should "declare the former and the current name of a case class" in {
    namesOf(Probe.evolutions) shouldBe Set(
      classOf[Probe].getName,
      classOf[Probe].getName.replace("Probe", "FormerProbe"),
      // A deleted class name, with and without `$`: the deleted class can be a class or an object
      classOf[Probe].getName.replace("Probe", "GoneType"),
      classOf[Probe].getName.replace("Probe", "GoneType$")
    )
  }

  it should "declare a sealed trait and the former name it was renamed from" in {
    namesOf(Pet.evolutions) should contain allOf (
      classOf[Pet].getName,
      classOf[Pet].getName.replace("Pet", "Animal")
    )
  }

  // Each ADT evolution holds only what migrates its version: version 1 gets the add of version 2 and the mapper
  it should "compute the evolution of the data written at each version of a class name" in {
    val declaration = Probe.evolutions.declarations.find(_.className == classOf[Probe].getName).get

    declaration.versions.map(_.formerVersion) shouldBe Seq(0, 1, 2)
    lineageOf(declaration.versions(1), Array("id", "count")) shouldBe Right(Seq(Some(0), Some(1), None))
    val version1 = declaration.versions(1).asInstanceOf[AdtEvolution[Probe]]
    version1.migrate(Probe("id", "3", "default")) shouldBe Probe("id", "3", "default1")
    declaration.versions(2).isAsIs(Some(Array("id", "count", "label"))) shouldBe true
  }

  // The field evolutions: a rename, a transform with its mapper, a delete, and an add with its default value
  it should "plan the field evolutions migrating the data written under a former name" in {
    val formerName = classOf[Probe].getName.replace("Probe", "FormerProbe")
    val evolution  = Evolutions.get[Probe](formerName, 0, getClass.getClassLoader)

    // The transformed field and the added field read no former field directly
    lineageOf(evolution, Array("formerId", "count", "gone")) shouldBe Right(Seq(Some(0), None, None))
  }

  // The mappers are private to the companion: the evolutions are built in the companion, where the mappers are visible
  it should "apply the mappers of the companion" in {
    evolutionOf(classOf[Probe], 1).migrate(Probe("id", "3", "default")) shouldBe Probe("id", "3", "default1")
  }

  // The plan checks the field names during the schema compatibility resolution, which only logs these messages
  it should "report an unused former field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongFieldNotUsed]) shouldBe Seq(
      "Former field 'b' is not in the current schema. Declare @deletedFields(since = <version>, \"b\") or @renamed"
    )
  }

  it should "report every offending field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongSeveralFields]) shouldBe Seq(
      "Field 'missing' is not in the former data. Declare @added(since = <version>) or @renamed on it",
      "Former field 'identifier' is not in the current schema. Declare @deletedFields(since = <version>, \"identifier\") or @renamed",
      "Former field 'b' is not in the current schema. Declare @deletedFields(since = <version>, \"b\") or @renamed"
    )
  }

  it should "report a missing current field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongMissingField]) shouldBe Seq(
      "Field 'missingField' is not in the former data. Declare @added(since = <version>) or @renamed on it"
    )
  }

  // An unversioned ADT has the No evolution, whose plan also reports a field added without @added
  it should "report a field added without annotation to an unversioned case class" in {
    evolutionOf(classOf[AddedFieldWithoutAnnotation]).plan(Array("a")).swap shouldBe Right(
      Seq("Field 'b' is not in the former data. Declare @added(since = <version>) or @renamed on it")
    )
  }

}

object EvolutionMacroTest {

  /* Probe at version 0
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
    // Visible to the annotations of the class only
    private[EvolutionMacroTest] def intToString(i: Int): String             = i.toString
    private[EvolutionMacroTest] def bump(version: Int, probe: Probe): Probe = probe.copy(label = probe.label + version)
  }

}
