package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Restores the data a former source code wrote: the serializer restored from its snapshot reads it, applying the
  * declared evolutions. The former code writing each snapshot file is kept, commented, next to the current one, with
  * the test generating the file.
  */
class EvolutionTest extends AnyFlatSpec with Matchers with TestUtils {

  import org.apache.flinkx.api.evolution.EvolutionTest._

  /** Reads the data of the given snapshot file with the serializer restored from its snapshot, once shipped through
    * Java serialization, as Flink may ship a serializer.
    */
  private def deserializeShippedFromFile[T](fileName: String): T = {
    val input    = new DataInputViewStreamWrapper(snapshotInput(fileName))
    val restored = TypeSerializerSnapshot.readVersionedSnapshot[T](input, getClass.getClassLoader).restoreSerializer()
    input.readInt() // Size of the snapshot
    InstantiationUtil.clone(restored, getClass.getClassLoader).deserialize(input)
  }

  /** Reads the record of the Click v0 snapshot file, migrating it with the evolutions `T` declares from version 0. */
  private def readClickV0As[T <: Product](clazz: Class[T]): T = {
    val input   = new DataInputViewStreamWrapper(snapshotInput("Click-v0"))
    val clickV0 =
      TypeSerializerSnapshot.readVersionedSnapshot[Click](input, getClass.getClassLoader).restoreSerializer()
    input.readInt() // Size of the snapshot
    clickV0As(clickV0, clazz).deserialize(input)
  }

  /* Test to serialize Click v0 code into Click-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Click v0" in {
    val event: Click = Click("a", List(ClickEvent("2021-01-01", 1), ClickEvent("2022-02-02", 2)), 5, "id1", "b")
    serializeToFile("Click-v0", event)
  }
   */

  it should "deserialize Click v0 to Click v3" in {
    testDeserializeFromFile("Click-v0", ClickV3)
  }

  /* Test to serialize Event v0 code into Event-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Event v0" in {
    val event: Event = View(123456789)
    serializeToFile("Event-v0", event)
  }
   */

  it should "deserialize Event v0 to Action v1" in {
    testDeserializeFromFile[Action]("Event-v0", Web(123456790))
  }

  // A serializer must survive Java serialization, the one restored from a savepoint too, with the mappers it applies
  it should "apply @transformed and @postEvolution with a restored serializer shipped through Java serialization" in {
    deserializeShippedFromFile[Click]("Click-v0") shouldBe ClickV3
    deserializeShippedFromFile[Action]("Event-v0") shouldBe Web(123456790)
  }

  // Each former version replays only the deletions that came after it, one of them deleting a field renamed in between
  it should "deserialize Dog v0 to Dog v2" in {
    val (_, restored) = deserializeFormerFields(
      classOf[Dog],
      0,
      ("name", StringSerializer.INSTANCE, "Beethoven"),
      ("kind", StringSerializer.INSTANCE, "St. Bernard")
    )
    restored shouldBe Dog("Beethoven")
  }

  it should "deserialize Dog v1 to Dog v2" in {
    val (_, restored) = deserializeFormerFields(
      classOf[Dog],
      1,
      ("name", StringSerializer.INSTANCE, "Beethoven"),
      ("breed", StringSerializer.INSTANCE, "St. Bernard")
    )
    restored shouldBe Dog("Beethoven")
  }

  /* Test to serialize Animal v0 code into Animal-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Animal v0" in {
    val animal: Animal = Horse("Spirit")
    serializeToFile("Animal-v0", animal)
  }
   */

  it should "deserialize Animal v0 to null, its subtype being deleted" in {
    testDeserializeFromFile[Animal]("Animal-v0", null)
  }

  // Horse was deleted in version 1, then a class took its name again in version 2
  it should "deserialize a subtype bearing the name of a subtype deleted before" in {
    roundtrip[Animal](createSerializer[Animal], Horse(4))
  }

  // The default of @deletedClasses: deleting a class still in use loses data, unless the restore is told to
  it should "throw when deserializing an instance of a deleted subtype" in {
    val tapeName         = classOf[Shelf].getName.replace("Shelf", "Tape")
    val formerSerializer = new CoproductSerializer[AnyRef](
      evolution = Some(evolutionOf(classOf[Shelf]).asInstanceOf[Evolution[AnyRef]]),
      subtypeClasses = Array[Class[_]](classOf[Book], classOf[FormerTape]),
      subtypeFqns = Array(classOf[Book].getName, tapeName),
      subtypeSerializers = Array[TypeSerializer[_]](
        createSerializer[Book],
        new CaseClassSerializer[FormerTape](
          new Evolution(tapeName, 0, classOf[FormerTape]),
          isCaseClassImmutable = true,
          fieldNames = Array("title"),
          paramSerializers = Array(StringSerializer.INSTANCE)
        )
      )
    )

    deserializeFormerForm[Shelf](formerSerializer, Book("Dune")) shouldBe Book("Dune")
    val exception =
      intercept[DeletedInstanceException](deserializeFormerForm[Shelf](formerSerializer, FormerTape("Up")))
    exception.getMessage should startWith(
      s"Encountered an instance of deleted '$tapeName' class during deserialization"
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
    val (snapshot, restored) = deserializeFormerFields(classOf[Ticket], 0, ("id", StringSerializer.INSTANCE, "t"))

    restored shouldBe Ticket("t@0")
    createSerializer[Ticket].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  // Whichever serializer Flink reads with, the restored or the current one, an evolution never applies to such snapshot
  it should "not apply @postEvolution to a case class snapshot recording no field name" in {
    val formerSerializer = new CaseClassSerializer[Ticket](
      evolution = evolutionOf(classOf[Ticket]),
      isCaseClassImmutable = true,
      fieldNames = Array.empty,
      paramSerializers = Array(createSerializer[String])
    )

    compatibilityWith[Ticket](formerSerializer) shouldBe Symbol("compatibleAsIs")
    deserializeFormerForm[Ticket](formerSerializer, Ticket("t")) shouldBe Ticket("t")
  }

  // A default value is evaluated for each restored instance, as the constructor does, not once for all of them
  it should "evaluate the default value of an added field for each restored instance" in {
    val (_, first)  = deserializeFormerFields(classOf[Stamped], 0, ("id", StringSerializer.INSTANCE, "a"))
    val (_, second) = deserializeFormerFields(classOf[Stamped], 0, ("id", StringSerializer.INSTANCE, "b"))

    second.stamp shouldNot be(first.stamp)
  }

  it should "throw when deserializing Click v0 with a wrong added field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongAddedField]))
    exception.getMessage shouldBe s"Cannot migrate ${classOf[WrongAddedField]} from version 0: Cannot add 'a': field" +
      " already exists. Existing fields: [\"a\"]"
  }

  it should "throw when deserializing Click v0 with a wrong renamed field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongRenamedField]))
    exception.getMessage shouldBe s"Cannot migrate ${classOf[WrongRenamedField]} from version 0: Cannot rename" +
      " 'wrongFieldName': field not found. Available fields: [\"a\"]"
  }

  it should "throw when deserializing Click v0 with a wrong transformed field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongTransformedField]))
    exception.getMessage shouldBe s"Cannot migrate ${classOf[WrongTransformedField]} from version 0: Cannot transform" +
      " 'wrongFieldName': field not found. Available fields: [\"a\"]"
  }

  // Restoring the oldest versions replays every deletion: a field renamed then deleted is no longer found
  it should "ignore a deleted field not found when deserializing Click v0" in {
    readClickV0As(classOf[WrongDeletedField]) shouldBe WrongDeletedField("a")
  }

}

object EvolutionTest {

  /** The serializer reading the data of Click v0 its restored serializer reads, migrating it with the evolutions `T`
    * declares from version 0.
    */
  def clickV0As[T <: Product](clickV0: TypeSerializer[Click], clazz: Class[T]): CaseClassSerializer[T] = {
    val serializer = clickV0.asInstanceOf[CaseClassSerializer[Click]]
    new CaseClassSerializer[T](
      evolution = evolutionOf(clazz),
      isCaseClassImmutable = serializer.isCaseClassImmutable,
      fieldNames = serializer.fieldNames,
      paramSerializers = serializer.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]
    )
  }

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
  object Click extends Evolving[Click] { val evolutions = Evolutions[Click] }

  @version(1)
  @renamed(since = 1, "ClickEvent")
  case class ClickAction(
      @renamed(since = 1, "sessionId") @transformed(since = 1, mapIntToString) id: String,
      date: String
  )
  object ClickAction extends Evolving[ClickAction] { val evolutions = Evolutions[ClickAction] }

  /** Click v0 of the snapshot file, migrated to Click v3. */
  val ClickV3: Click = Click("id1", List(ClickAction("1", "2021-01-01"), ClickAction("2", "2022-02-02")), 25, "5")

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
  object Action extends Evolving[Action] { val evolutions = Evolutions[Action] }

  @version(0)
  case object Login extends Action

  @version(1)
  @renamed(since = 1, "View")
  case class Web(ts: Long) extends Action
  object Web               extends Evolving[Web] { val evolutions = Evolutions[Web] }

  case class Cart(items: Int) extends Action

  def updateAction(version: Int, action: Action): Action = action match {
    case Web(ts) => Web(if (version == 0) ts + 1 else ts)
    case Cart(_) => Login
    case e @ _   => e
  }

  @version(1)
  @postEvolution(markTicket)
  case class Ticket(id: String)
  object Ticket extends Evolving[Ticket] { val evolutions = Evolutions[Ticket] }

  def markTicket(version: Int, ticket: Ticket): Ticket = ticket.copy(id = s"${ticket.id}@$version")

  @version(1)
  case class Stamped(id: String, @added(since = 1) stamp: Int = Stamped.next())
  object Stamped extends Evolving[Stamped] {
    val evolutions     = Evolutions[Stamped]
    private val stamps = new java.util.concurrent.atomic.AtomicInteger()
    def next(): Int    = stamps.incrementAndGet()
  }

  case class AddedFieldWithoutAnnotation(a: String, b: Int = 42)

  @version(1)
  @deletedFields(since = 1, "inFileClicks", "identifier", "b")
  case class WrongUntransformedField(@renamed(since = 1, "a") renamedA: String, fieldNotInFile: String)
  object WrongUntransformedField extends Evolving[WrongUntransformedField] {
    val evolutions = Evolutions[WrongUntransformedField]
  }

  @version(1)
  case class OuterEvolvedWithNested(@renamed(since = 1, "formerNested") nested: AddedFieldWithoutAnnotation)
  object OuterEvolvedWithNested extends Evolving[OuterEvolvedWithNested] {
    val evolutions = Evolutions[OuterEvolvedWithNested]
  }

  case class UnrelatedFormerCaseClass(a: String, removed: String)

  @version(1)
  @deletedFields(since = 1, "removed")
  case class UnrelatedCurrentCaseClass(a: String)
  object UnrelatedCurrentCaseClass extends Evolving[UnrelatedCurrentCaseClass] {
    val evolutions = Evolutions[UnrelatedCurrentCaseClass]
  }

  // Both traits are sealed in this file, so a single case class can be a member of each of them
  @version(1)
  sealed trait SharedSubtypeTrait
  object SharedSubtypeTrait extends Evolving[SharedSubtypeTrait] {
    val evolutions = Evolutions[SharedSubtypeTrait]
  }
  @version(1)
  sealed trait OtherSharedSubtypeTrait
  object OtherSharedSubtypeTrait extends Evolving[OtherSharedSubtypeTrait] {
    val evolutions = Evolutions[OtherSharedSubtypeTrait]
  }
  case class SharedSubtype(a: String) extends SharedSubtypeTrait with OtherSharedSubtypeTrait

  @version(1)
  @renamed(since = 1, "FormerRenamedCaseClass")
  @deletedFields(since = 1, "removed")
  case class RenamedCaseClass(a: String)
  object RenamedCaseClass extends Evolving[RenamedCaseClass] { val evolutions = Evolutions[RenamedCaseClass] }

  /* RenamedCaseClass v0
  case class FormerRenamedCaseClass(a: String, removed: String)
   */

  @version(2)
  sealed trait RolledBackTrait
  object RolledBackTrait            extends Evolving[RolledBackTrait] { val evolutions = Evolutions[RolledBackTrait] }
  case class RolledBackA(a: String) extends RolledBackTrait
  case class RolledBackB(b: Int)    extends RolledBackTrait

  @version(1)
  sealed trait SubtypeRemovedWithoutAnnotation
  object SubtypeRemovedWithoutAnnotation extends Evolving[SubtypeRemovedWithoutAnnotation] {
    val evolutions = Evolutions[SubtypeRemovedWithoutAnnotation]
  }
  case class RemainingSubtype(a: String) extends SubtypeRemovedWithoutAnnotation
  case class RemovedSubtype(b: Int)      extends SubtypeRemovedWithoutAnnotation
  /* Dog v0
  case class Dog(name: String, kind: String)
   */

  /* Dog v1
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
  object Dog extends Evolving[Dog] { val evolutions = Evolutions[Dog] }

  /* Animal-v0
  sealed trait Animal
  case class Horse(name: String) extends Animal
  case class Lion(name: String) extends Animal
   */

  /* Animal v1
  @version(1)
  @deletedClasses(since = 1, "Horse")
  sealed trait Animal
  case class Lion(name: String) extends Animal
   */

  @version(2)
  @deletedClasses(since = 1, throwOnInstance = false, "Horse")
  sealed trait Animal
  object Animal extends Evolving[Animal] { val evolutions = Evolutions[Animal] }
  @version(2)
  case class Horse(legs: Int)   extends Animal
  object Horse                  extends Evolving[Horse] { val evolutions = Evolutions[Horse] }
  case class Lion(name: String) extends Animal

  /* Shelf v0
  sealed trait Shelf
  case class Book(title: String) extends Shelf
  case class Tape(title: String) extends Shelf
   */

  @version(1)
  @deletedClasses(since = 1, "Tape")
  sealed trait Shelf
  object Shelf                   extends Evolving[Shelf] { val evolutions = Evolutions[Shelf] }
  case class Book(title: String) extends Shelf

  // Stands for Tape in the data of Shelf v0, under a name of its own: no class may bear the name of a deleted class
  case class FormerTape(title: String)

}
