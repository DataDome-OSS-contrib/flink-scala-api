package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.EvolutionTest._

  // Schema of Click v0, as recorded in its snapshot, see the commented out fixture below
  private val ClickV0FieldNames = Array("a", "inFileClicks", "fieldNotInFile", "identifier", "b")
  // Former class name of Click v0, as recorded in its snapshot and declared by @renamed on the fixtures below
  private val ClickV0ClassName = classOf[Click].getName

  /** Messages of every failure the dry run of the `formerClassName` evolutions reports from the given former field
    * names.
    *
    * The evolutions are registered under the former class name the snapshot records, which is where the restore looks
    * them up: the current class name only resolves from the version of the rename onwards.
    */
  private def dryRunFailures(formerClassName: String, formerFieldNames: Array[String]): Seq[String] =
    Evolutions
      .find[Any](formerClassName, 0, getClass.getClassLoader)
      .getOrElse(fail(s"No evolution registered for $formerClassName"))
      .dryRun(formerFieldNames)
      .swap
      .map(_.map(_.getMessage))
      .getOrElse(Seq.empty)

  override protected def beforeEach(): Unit = {
    auto.cache.clear()
    Evolutions.reset()
  }

  /* Test to serialize Click v0 code into Click-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Click v0" in {
    val event: Click = Click("a", List(ClickEvent("2021-01-01", 1), ClickEvent("2022-02-02", 2)), 5, "id1", "b")
    serializeToFile("Click-v0", event)
  }
   */

  it should "deserialize Click v0 to Click v3" in {
    val expected: Click = Click("id1", List(ClickAction("1", "2021-01-01"), ClickAction("2", "2022-02-02")), 25, "5")
    testDeserializeFromFile("Click-v0", expected)
  }

  /* Test to serialize Event v0 code into Event-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Event v0" in {
    val event: Event = View(123456789)
    serializeToFile("Event-v0", event)
  }
   */

  it should "deserialize Event v0 to Action v1" in {
    val expected: Action = Web(123456790)
    testDeserializeFromFile("Event-v0", expected)
  }

  // A state backend gates the restore on the schema compatibility resolution, before deserializing anything: the
  // evolutions are applied by the restored former serializer, so the state has to be migrated with it.
  it should "resolve the schema compatibility of Click v0 to Click v3 as compatible after migration" in {
    resolveSchemaCompatibilityFromFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of Event v0 to Action v1 as compatible after migration" in {
    resolveSchemaCompatibilityFromFile[Action]("Event-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of a state of Click v0 as compatible after migration" in {
    // The value serializer of a MapState[String, Click], nested in a serializer resolving its nested serializers
    resolveNestedSchemaCompatibilityFromFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  // A snapshot written before 2.4.0 carries no field name, so the fields are read by position: the evolutions can't
  // apply, and the standard resolution checks that positional layout.
  it should "resolve the schema compatibility of a pre-2.4.0 snapshot of an unchanged case class as compatible as is" in {
    val formerSerializer = new CaseClassSerializer[ClickAction](
      evolution = Evolutions.get(classOf[ClickAction], 0),
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array.empty,
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    resolveSchemaCompatibility(formerSerializer) shouldBe Symbol("compatibleAsIs")
  }

  it should "resolve the schema compatibility of an unevolved case class as compatible as is" in {
    resolveSchemaCompatibility[ClickAction](createSerializer[ClickAction]) shouldBe Symbol("compatibleAsIs")
  }

  it should "resolve the schema compatibility of an unevolved sealed trait as compatible as is" in {
    resolveSchemaCompatibility[Action](createSerializer[Action]) shouldBe Symbol("compatibleAsIs")
  }

  // Two unrelated ADTs must never be migrated into one another, even when the declared evolutions happen to line their
  // schemas up: the former and the current classes are compared, after resolveFormerClass mapped any rename.
  it should "resolve the schema compatibility of an unrelated case class as incompatible" in {
    val formerSerializer = new CaseClassSerializer[UnrelatedFormerCaseClass](
      evolution = Evolutions.get(classOf[UnrelatedFormerCaseClass], 0),
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array("a", "removed"),
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    resolveSchemaCompatibilityAfterRestore[UnrelatedCurrentCaseClass](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of an unrelated sealed trait as incompatible" in {
    val formerSerializer = createSerializer[SharedSubtypeTrait]

    resolveSchemaCompatibilityAfterRestore[OtherSharedSubtypeTrait](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a foreign snapshot as incompatible" in {
    resolveSchemaCompatibility[Action](createSerializer[Click].asInstanceOf[TypeSerializer[Action]]) shouldBe Symbol(
      "incompatible"
    )
  }

  // @postEvolution migrates the data of a former version only: the data of the current version is read as it was written
  it should "not apply @postEvolution to a case class written at the current version" in {
    roundtrip(createSerializer[Click], Click("id", Nil, 7, "5"))
  }

  it should "not apply @postEvolution to a sealed trait written at the current version" in {
    roundtrip(createSerializer[Action], Cart(2))
  }

  // A version raised for a @postEvolution alone changes nothing in the schema, yet the state must still be migrated
  it should "apply @postEvolution to a case class written at a former version with an unchanged schema" in {
    val formerSerializer = new CaseClassSerializer[Ticket](
      evolution = Evolutions.get(classOf[Ticket], 0),
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array("id"),
      paramSerializers = Array(createSerializer[String])
    )

    resolveSchemaCompatibilityAfterRestore[Ticket](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
    deserializeFormerForm(formerSerializer, Ticket("t")) shouldBe Ticket("t@0")
  }

  // Error handling
  /* Test to serialize Dog v0 code into Dog-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Dog v0" in {
    val dog: Dog = Dog("Beethoven", "St. Bernard")
    serializeToFile("Dog-v0", dog)
  }
   */

  it should "deserialize Dog v0 to Dog v2" in {
    val expected: Dog = Dog("Beethoven")
    testDeserializeFromFile("Dog-v0", expected)
  }

  /* Test to serialize Dog v1 code into Dog-v1.snapshot file, uncomment both test and code to regenerate
  it should "serialize Dog v1" in {
    val dog: Dog = Dog("Beethoven", "St. Bernard")
    serializeToFile("Dog-v1", dog)
  }
   */

  it should "deserialize Dog v1 to Dog v2" in {
    val expected: Dog = Dog("Beethoven")
    testDeserializeFromFile("Dog-v1", expected)
  }

  /* Test to serialize Dog v1 code into Dog-v1.snapshot file, uncomment both test and code to regenerate
  it should "serialize Animal v0" in {
    val animal: Animal = Horse("Spirit")
    serializeToFile("Animal-v0", animal)
  }
   */

  /* Test to serialize Dog v1 code into Dog-v1.snapshot file, uncomment both test and code to regenerate
  it should "serialize Animal v2" in {
    val animal: Animal = Horse(4)
    serializeToFile("Animal-v2", animal)
  }
   */

  it should "deserialize Animal v0 to null" in {
    val expected: Animal = null
    testDeserializeFromFile("Animal-v0", expected)
  }

  it should "deserialize Animal v2" in {
    val expected: Animal = Horse(4)
    testDeserializeFromFile("Animal-v2", expected)
  }

}

object EvolutionTest {

  // API demonstration

  /* click-v0
  case class Click(
      a: String,
      inFileClicks: List[ClickEvent],
      fieldNotInFile: Int,
      identifier: String,
      b: String
  )

  case class ClickEvent(
      date: String,
      sessionId: Int
  )
   */

  @version(3)
  @deletedFields(since = 1, "a")
  @deletedFields(since = 2, "b")
  @postEvolution(updateClick)
  case class Click(
      @renamed(since = 2, "identifier") id: String,
      inFileClicks: List[ClickAction],
      @added(since = 3) fieldInFile: Int = 1,
      @transformed(since = 1, mapIntToString) fieldNotInFile: String
  )
  object Click extends Evolved[Click]

  @version(1)
  @renamed(since = 1, "ClickEvent")
  case class ClickAction(
      @renamed(since = 1, "sessionId") @transformed(since = 1, mapIntToString) id: String,
      date: String
  )
  object ClickAction extends Evolved[ClickAction]

  def mapIntToString(a: Int): String                 = a.toString
  def updateClick(version: Int, click: Click): Click =
    click.copy(fieldInFile = click.fieldNotInFile.toInt * 5 + version)

  /* event-v0
  sealed trait Event
  case class View(ts: Long) extends Event
  case class Purchase(price: Double) extends Event
   */

  @version(1)
  @renamed(since = 1, "Event")
  @deletedClasses(since = 1, "Purchase")
  @postEvolution(updateAction)
  sealed trait Action
  object Action extends Evolved[Action]

  @version(0)
  case object Login extends Action

  @version(1)
  @renamed(since = 1, "View")
  case class Web(ts: Long) extends Action
  object Web               extends Evolved[Web]

  case class Cart(items: Int) extends Action

  def updateAction(version: Int, action: Action): Action = action match {
    case Web(ts) => Web(if (version == 0) ts + 1 else ts)
    case Cart(_) => Login
    case e @ _   => e
  }

  @version(1)
  @postEvolution(markTicket)
  case class Ticket(id: String)
  object Ticket extends Evolved[Ticket]

  def markTicket(version: Int, ticket: Ticket): Ticket = ticket.copy(id = s"${ticket.id}@$version")

  case class AddedFieldWithoutAnnotation(a: String, b: Int = 42)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongUntransformedField(@renamed(since = 1, "a") renamedA: String, fieldNotInFile: String)
  object WrongUntransformedField extends IsolatedEvolved[WrongUntransformedField]

  @version(1)
  case class OuterEvolvedWithNested(@renamed(since = 1, "formerNested") nested: AddedFieldWithoutAnnotation)
  object OuterEvolvedWithNested extends Evolved[OuterEvolvedWithNested]

  case class UnrelatedFormerCaseClass(a: String, removed: String)

  @version(1)
  @deletedFields(since = 1, "removed")
  case class UnrelatedCurrentCaseClass(a: String)
  object UnrelatedCurrentCaseClass extends Evolved[UnrelatedCurrentCaseClass]

  // Both traits are sealed in this file, so a single case class can be a member of each of them
  @version(1)
  sealed trait SharedSubtypeTrait
  object SharedSubtypeTrait extends Evolved[SharedSubtypeTrait]
  @version(1)
  sealed trait OtherSharedSubtypeTrait
  object OtherSharedSubtypeTrait      extends Evolved[OtherSharedSubtypeTrait]
  case class SharedSubtype(a: String) extends SharedSubtypeTrait with OtherSharedSubtypeTrait

  @version(1)
  @renamed(since = 1, "FormerRenamedCaseClass")
  @deletedFields(since = 1, "removed")
  case class RenamedCaseClass(a: String)
  object RenamedCaseClass extends Evolved[RenamedCaseClass]

  // Still declared to play the former class: the snapshot records its name, which the rename resolves
  case class FormerRenamedCaseClass(a: String, removed: String)

  @version(2)
  sealed trait RolledBackTrait
  object RolledBackTrait            extends Evolved[RolledBackTrait]
  case class RolledBackA(a: String) extends RolledBackTrait
  case class RolledBackB(b: Int)    extends RolledBackTrait

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class FirstClaimingFormerName(a: String)
  object FirstClaimingFormerName extends IsolatedEvolved[FirstClaimingFormerName]

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class SecondClaimingFormerName(a: String)
  object SecondClaimingFormerName extends IsolatedEvolved[SecondClaimingFormerName]

  @version(1)
  @renamed(since = 1, "ClaimedByBoth")
  case class ClaimingFormerNameByRename(a: String)
  object ClaimingFormerNameByRename extends IsolatedEvolved[ClaimingFormerNameByRename]

  @version(1)
  @deletedFields(since = 1, "removed")
  @deletedClasses(since = 1, throwOnInstance = false, "ClaimedByBoth")
  case class ClaimingFormerNameByDeletion(a: String)
  object ClaimingFormerNameByDeletion extends IsolatedEvolved[ClaimingFormerNameByDeletion]

  @version(1)
  sealed trait SubtypeRemovedWithoutAnnotation
  object SubtypeRemovedWithoutAnnotation extends Evolved[SubtypeRemovedWithoutAnnotation]
  case class RemainingSubtype(a: String) extends SubtypeRemovedWithoutAnnotation
  case class RemovedSubtype(b: Int)      extends SubtypeRemovedWithoutAnnotation
  /* Dog-v0
  case class Dog(name: String, kind: String)
   */

  /* Dog-v1
  @version(1)
  case class Dog(
    name: String,
    @renamed(since = 1, "kind") breed: String
  )
   */

  @version(2)
  @deletedFields(since = 1, "kind")
  @deletedFields(since = 2, "breed")
  case class Dog(name: String)
  object Dog extends Evolved[Dog]

  /* Animal-v0
  sealed trait Animal
  case class Horse(name: String) extends Animal
  case class Lion(name: String) extends Animal
   */

  /* Animal-v1
  @version(1)
  @deletedClasses(since = 1, "Horse")
  sealed trait Animal
  case class Lion(name: String) extends Animal
   */

  @version(2)
  @deletedClasses(since = 1, throwOnInstance = false, "Horse")
  sealed trait Animal
  object Animal extends Evolved[Animal]
  @version(2)
  case class Horse(legs: Int)   extends Animal
  object Horse                  extends Evolved[Horse]
  case class Lion(name: String) extends Animal

}
