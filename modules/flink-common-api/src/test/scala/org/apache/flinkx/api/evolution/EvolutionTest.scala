package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.base.{IntSerializer, StringSerializer}
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataInputViewStreamWrapper, DataOutputSerializer}
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.AdtEvolution.DeletedEvolution
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ListSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Restores the former data of the snapshot files: the restored serializer reads them and applies the evolutions. Each
  * file has, in comments, the test that wrote it and its former source code, next to the current source code.
  */
class EvolutionTest extends AnyFlatSpec with Matchers with TestUtils {

  import org.apache.flinkx.api.evolution.EvolutionTest._

  /** Reads the former data of the given snapshot file with the restored serializer, after a Java serialization of the
    * restored serializer, as Flink can do.
    */
  private def deserializeShippedFromFile[T](fileName: String): T = {
    val input    = new DataInputViewStreamWrapper(snapshotInput(fileName))
    val restored = TypeSerializerSnapshot.readVersionedSnapshot[T](input, getClass.getClassLoader).restoreSerializer()
    input.readInt() // Size of the snapshot
    InstantiationUtil.clone(restored, getClass.getClassLoader).deserialize(input)
  }

  /** Reads the record of Click-v0.snapshot with the ADT evolution of `T` for the data written at version 0. */
  private def readClickV0As[T <: Product](clazz: Class[T]): T = {
    val input   = new DataInputViewStreamWrapper(snapshotInput("Click-v0"))
    val clickV0 =
      TypeSerializerSnapshot.readVersionedSnapshot[Click](input, getClass.getClassLoader).restoreSerializer()
    input.readInt() // Size of the snapshot
    clickV0As(clickV0, clazz).deserialize(input)
  }

  /* Uncomment this test and Click at version 0 to write Click-v0.snapshot again
  it should "serialize Click v0" in {
    val event: Click = Click("a", List(ClickEvent("2021-01-01", 1), ClickEvent("2022-02-02", 2)), 5, "id1", "b")
    serializeToFile("Click-v0", event)
  }
   */

  it should "deserialize Click v0 to Click v3" in {
    testDeserializeFromFile("Click-v0", ClickV3)
  }

  /* Uncomment this test and Event at version 0 to write Event-v0.snapshot again
  it should "serialize Event v0" in {
    val event: Event = View(123456789)
    serializeToFile("Event-v0", event)
  }
   */

  it should "deserialize Event v0 to Action v1" in {
    testDeserializeFromFile[Action]("Event-v0", Web(123456790))
  }

  // A restored serializer and its mappers must survive a Java serialization, as a current serializer does
  it should "apply @transformed and @postEvolution with a restored serializer shipped through Java serialization" in {
    deserializeShippedFromFile[Click]("Click-v0") shouldBe ClickV3
    deserializeShippedFromFile[Action]("Event-v0") shouldBe Web(123456790)
  }

  // Each plan applies only the deletions above its former version, one of them on a renamed field
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

  /* Uncomment this test and Animal at version 0 to write Animal-v0.snapshot again
  it should "serialize Animal v0" in {
    val animal: Animal = Horse("Spirit")
    serializeToFile("Animal-v0", animal)
  }
   */

  it should "deserialize Animal v0 to null, its subtype being deleted" in {
    testDeserializeFromFile[Animal]("Animal-v0", null)
  }

  // Version 1 deletes Horse, and a class of version 2 reuses its name
  it should "deserialize a subtype that reuses the name of a subtype deleted at a lower version" in {
    roundtrip[Animal](infoToSer[Animal], Horse(4))
  }

  it should "deserialize a Seq of a subtype that reuses the name of a subtype deleted at a lower version" in {
    roundtrip[Seq[Horse]](infoToSer[Seq[Horse]], Seq(Horse(4)))
  }

  // By default, a deleted instance throws: the migration discards data only with throwOnInstance = false
  it should "throw when deserializing an instance of a deleted subtype" in {
    val tapeName         = classOf[Shelf].getName.replace("Shelf", "Tape")
    val formerSerializer = new CoproductSerializer[AnyRef](
      evolution = Some(evolutionOf(classOf[Shelf]).asInstanceOf[AdtEvolution[AnyRef]]),
      subtypeClasses = Array[Class[_]](classOf[Book], classOf[FormerTape]),
      subtypeNames = Array(classOf[Book].getName, tapeName),
      subtypeSerializers = Array[TypeSerializer[_]](
        infoToSer[Book],
        new CaseClassSerializer[FormerTape](
          new AdtEvolution(tapeName, 0, classOf[FormerTape]),
          isCaseClassImmutable = true,
          fieldNames = Some(Array("title")),
          paramSerializers = Array(StringSerializer.INSTANCE)
        )
      )
    )

    deserializeFormerForm[Shelf](formerSerializer, Book("Dune")) shouldBe Book("Dune")
    val exception =
      intercept[DeletedInstanceException](deserializeFormerForm[Shelf](formerSerializer, FormerTape("Up")))
    exception.getMessage should startWith(
      s"The former data contains an instance of '$tapeName', a deleted class or enum value."
    )
  }

  it should "deserialize an instance of a deleted sealed trait to null" in {
    val serializer = new CoproductSerializer[AnyRef](
      evolution = Some(new DeletedEvolution[AnyRef](classOf[Shelf].getName, 0, throwOnInstance = false)),
      subtypeClasses = Array[Class[_]](classOf[Book]),
      subtypeNames = Array(classOf[Book].getName),
      subtypeSerializers = Array[TypeSerializer[_]](infoToSer[Book])
    )
    val out = new DataOutputSerializer(64)
    serializer.serialize(Book("Dune"), out)

    serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer)) shouldBe null
  }

  // The post-evolution mapper applies only to former data: data written at the current version reads as is
  it should "not apply @postEvolution to a case class written at the current version" in {
    roundtrip(infoToSer[Click], Click("id", Nil, 7, "5"))
  }

  it should "not apply @postEvolution to a sealed trait written at the current version" in {
    roundtrip(infoToSer[Action], Cart(2))
  }

  // A higher version with only @postEvolution does not change the schema, but the state must still migrate
  it should "apply @postEvolution to a case class written at a former version with an unchanged schema" in {
    val (snapshot, restored) = deserializeFormerFields(classOf[Ticket], 0, ("id", StringSerializer.INSTANCE, "t"))

    restored shouldBe Ticket("t@0")
    infoToSer[Ticket].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  // The evolutions never apply to a snapshot without field names, with the restored or the current serializer
  it should "not apply @postEvolution to a case class snapshot recording no field name" in {
    val formerSerializer = new CaseClassSerializer[Ticket](
      evolution = evolutionOf(classOf[Ticket]),
      isCaseClassImmutable = true,
      fieldNames = None,
      paramSerializers = Array(infoToSer[String])
    )

    compatibilityWith[Ticket](formerSerializer) shouldBe Symbol("compatibleAsIs")
    deserializeFormerForm[Ticket](formerSerializer, Ticket("t")) shouldBe Ticket("t")
  }

  // A case class without field records no field name either, but its snapshot has no field serializer to read by position
  it should "add a field to a case class that had none" in {
    val (snapshot, restored) = deserializeFormerFields(classOf[Flag], 0)

    restored shouldBe Flag(on = true)
    infoToSer[Flag].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  // The migration evaluates the default value for each instance, as the constructor does, not one time for all
  it should "evaluate the default value of an added field for each restored instance" in {
    val (_, first)  = deserializeFormerFields(classOf[Stamped], 0, ("id", StringSerializer.INSTANCE, "a"))
    val (_, second) = deserializeFormerFields(classOf[Stamped], 0, ("id", StringSerializer.INSTANCE, "b"))

    second.stamp shouldNot be(first.stamp)
  }

  it should "throw when deserializing Click v0 with a wrong added field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongAddedField]))
    exception.getMessage shouldBe s"Cannot migrate ${classOf[WrongAddedField]} from version 0: Cannot add field 'a':" +
      " it is already one of [\"a\"]"
  }

  it should "throw when deserializing Click v0 with a wrong renamed field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongRenamedField]))
    exception.getMessage shouldBe s"Cannot migrate ${classOf[WrongRenamedField]} from version 0: Cannot rename field" +
      " 'wrongFieldName': it is not one of [\"a\"]"
  }

  it should "throw when deserializing Click v0 with a wrong transformed field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongTransformedField]))
    exception.getMessage shouldBe s"Cannot migrate ${classOf[WrongTransformedField]} from version 0: Cannot transform" +
      " field 'wrongFieldName': it is not one of [\"a\"]"
  }

  // Without @deletedFields, the rename would discard the former value of 'a'
  it should "not restore a field renamed to the name of an undeclared deleted field" in {
    val exception = intercept[MigrationException](
      deserializeFormerFields(
        classOf[RenamedOverDeleted],
        0,
        ("a", IntSerializer.INSTANCE, 1),
        ("b", IntSerializer.INSTANCE, 2)
      )
    )
    exception.getMessage should include("Cannot rename field 'a': it is already one of")
  }

  it should "restore a field renamed to the name of a deleted field" in {
    val (snapshot, restored) = deserializeFormerFields(
      classOf[RenamedOverDeclaredDeleted],
      0,
      ("a", IntSerializer.INSTANCE, 1),
      ("b", IntSerializer.INSTANCE, 2)
    )

    restored shouldBe RenamedOverDeclaredDeleted(2)
    infoToSer[RenamedOverDeclaredDeleted].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  // Version 2 renames 'a' to 'b', then version 3 renames 'b' to 'c'
  it should "restore a field renamed twice" in {
    forAll(Seq(1 -> "a", 2 -> "b")) { case (version, formerName) =>
      val (snapshot, restored) =
        deserializeFormerFields(classOf[RenamedTwice], version, (formerName, StringSerializer.INSTANCE, "x"))

      restored shouldBe RenamedTwice("x")
      infoToSer[RenamedTwice].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
        "compatibleAfterMigration"
      )
    }
  }

  // Version 1 adds 'id', then version 2 renames it to 'ident'
  it should "restore a field added, then renamed" in {
    val (_, fromV0) = deserializeFormerFields(classOf[AddedThenRenamed], 0, ("name", StringSerializer.INSTANCE, "n"))
    val (_, fromV1) = deserializeFormerFields(
      classOf[AddedThenRenamed],
      1,
      ("name", StringSerializer.INSTANCE, "n"),
      ("id", StringSerializer.INSTANCE, "i")
    )

    fromV0 shouldBe AddedThenRenamed("n", "none")
    fromV1 shouldBe AddedThenRenamed("n", "i")
  }

  // Version 1 transforms 'sessionId', then version 2 renames it to 'id'
  it should "restore a field transformed, then renamed" in {
    val (_, fromV0) =
      deserializeFormerFields(classOf[TransformedThenRenamed], 0, ("sessionId", StringSerializer.INSTANCE, "ab"))
    val (_, fromV1) =
      deserializeFormerFields(classOf[TransformedThenRenamed], 1, ("sessionId", StringSerializer.INSTANCE, "cd"))

    fromV0 shouldBe TransformedThenRenamed("AB")
    fromV1 shouldBe TransformedThenRenamed("cd")
  }

  // The migration skips the deleted field: it builds no instance of the deleted classes, which would throw
  it should "restore a case class whose deleted field holds instances of deleted classes" in {
    val (snapshot, restored) = deserializeFormerFields(
      classOf[Tapped],
      0,
      ("id", StringSerializer.INSTANCE, "a"),
      (
        "history",
        new ListSerializer[Product](formerTapEvent, classOf[Product]),
        List(FormerTapEvent("d", FormerTarget("t")))
      )
    )

    restored shouldBe Tapped("a")
    infoToSer[Tapped].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  // The deleted subtype reads as null without its fields: its field of a deleted class would throw
  it should "deserialize an instance of a deleted subtype that holds an instance of a deleted class to null" in {
    val cassetteName     = classOf[Rack].getName.replace("Rack", "Cassette")
    val formerSerializer = new CoproductSerializer[AnyRef](
      evolution = Some(evolutionOf(classOf[Rack]).asInstanceOf[AdtEvolution[AnyRef]]),
      subtypeClasses = Array[Class[_]](classOf[Disc], classOf[FormerCassette]),
      subtypeNames = Array(classOf[Disc].getName, cassetteName),
      subtypeSerializers = Array[TypeSerializer[_]](
        infoToSer[Disc],
        new CaseClassSerializer[Product](
          new AdtEvolution(cassetteName, 0, classOf[Product]),
          isCaseClassImmutable = true,
          fieldNames = Some(Array("target")),
          paramSerializers = Array(formerTarget)
        )
      )
    )

    deserializeFormerForm[Rack](formerSerializer, Disc("Dune")) shouldBe Disc("Dune")
    deserializeFormerForm[Rack](formerSerializer, FormerCassette(FormerTarget("t"))) shouldBe null
  }

  /** The serializer of the deleted class Target at version 0. FormerTarget takes its place. */
  private def formerTarget: TypeSerializer[_] =
    new CaseClassSerializer[Product](
      new AdtEvolution(classOf[Tapped].getName.replace("Tapped", "Target"), 0, classOf[Product]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("name")),
      paramSerializers = Array(StringSerializer.INSTANCE)
    )

  /** The serializer of the deleted class TapEvent at version 0. FormerTapEvent takes its place. */
  private def formerTapEvent: TypeSerializer[Product] =
    new CaseClassSerializer[Product](
      new AdtEvolution(classOf[Tapped].getName.replace("Tapped", "TapEvent"), 0, classOf[Product]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("date", "target")),
      paramSerializers = Array(StringSerializer.INSTANCE, formerTarget)
    )

  // The plan of the lowest version applies all the deletions: a field renamed, then deleted, is not found
  it should "ignore a deleted field not found when deserializing Click v0" in {
    readClickV0As(classOf[WrongDeletedField]) shouldBe WrongDeletedField("a")
  }

}

object EvolutionTest {

  /** A serializer that reads the former data of Click at version 0, with the ADT evolution of `T` for version 0. */
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

  /* Click at version 0, which wrote Click-v0.snapshot
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

  /** The Click of Click-v0.snapshot, migrated to version 3. */
  val ClickV3: Click = Click("id1", List(ClickAction("1", "2021-01-01"), ClickAction("2", "2022-02-02")), 25, "5")

  def mapIntToString(a: Int): String                 = a.toString
  def updateClick(version: Int, click: Click): Click =
    click.copy(fieldInFile = click.fieldNotInFile.toInt * 5 + version)

  /* Event at version 0, which wrote Event-v0.snapshot
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

  /* Flag at version 0
  case class Flag()
   */

  @version(1)
  case class Flag(@added(since = 1) on: Boolean = true)
  object Flag extends Evolving[Flag] { val evolutions = Evolutions[Flag] }

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

  case class UnrelatedFormerCaseClass(a: String, deleted: String)

  @version(1)
  @deletedFields(since = 1, "deleted")
  case class UnrelatedCurrentCaseClass(a: String)
  object UnrelatedCurrentCaseClass extends Evolving[UnrelatedCurrentCaseClass] {
    val evolutions = Evolutions[UnrelatedCurrentCaseClass]
  }

  // This file contains both sealed traits, so one case class can be a subtype of the two
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
  @deletedFields(since = 1, "deleted")
  case class RenamedCaseClass(a: String)
  object RenamedCaseClass extends Evolving[RenamedCaseClass] { val evolutions = Evolutions[RenamedCaseClass] }

  /* RenamedCaseClass at version 0
  case class FormerRenamedCaseClass(a: String, deleted: String)
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
  /* Dog at version 0
  case class Dog(name: String, kind: String)
   */

  /* Dog at version 1
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

  /* Animal at version 0, which wrote Animal-v0.snapshot
  sealed trait Animal
  case class Horse(name: String) extends Animal
  case class Lion(name: String) extends Animal
   */

  /* Animal at version 1
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

  /* Shelf at version 0
  sealed trait Shelf
  case class Book(title: String) extends Shelf
  case class Tape(title: String) extends Shelf
   */

  @version(1)
  @deletedClasses(since = 1, "Tape")
  sealed trait Shelf
  object Shelf                   extends Evolving[Shelf] { val evolutions = Evolutions[Shelf] }
  case class Book(title: String) extends Shelf

  // Takes the place of the deleted Tape in the former data, with another name to avoid a name reuse
  case class FormerTape(title: String)

  /* Tapped at version 0
  case class Tapped(id: String, history: List[TapEvent])
  case class TapEvent(date: String, target: Target)
  case class Target(name: String)
   */

  @version(1)
  @deletedFields(since = 1, "history")
  @deletedClasses(since = 1, "TapEvent", "Target")
  case class Tapped(id: String)
  object Tapped extends Evolving[Tapped] { val evolutions = Evolutions[Tapped] }

  /* Rack at version 0
  sealed trait Rack
  case class Disc(title: String) extends Rack
  case class Cassette(target: Target) extends Rack
   */

  @version(1)
  @deletedClasses(since = 1, throwOnInstance = false, "Cassette")
  sealed trait Rack
  object Rack                    extends Evolving[Rack] { val evolutions = Evolutions[Rack] }
  case class Disc(title: String) extends Rack

  // Take the place of the deleted classes in the former data
  case class FormerTapEvent(date: String, target: FormerTarget)
  case class FormerTarget(name: String)
  case class FormerCassette(target: FormerTarget)

  /* RenamedTwice at versions 0 and 1, then at version 2
  case class RenamedTwice(a: String)
  @version(2) case class RenamedTwice(@renamed(since = 2, "a") b: String)
   */

  @version(3)
  case class RenamedTwice(@renamed(since = 2, "a") @renamed(since = 3, "b") c: String)
  object RenamedTwice extends Evolving[RenamedTwice] { val evolutions = Evolutions[RenamedTwice] }

  /* AddedThenRenamed at version 0, then at version 1
  case class AddedThenRenamed(name: String)
  @version(1) case class AddedThenRenamed(name: String, @added(since = 1) id: String = "none")
   */

  @version(2)
  case class AddedThenRenamed(name: String, @added(since = 1) @renamed(since = 2, "id") ident: String = "none")
  object AddedThenRenamed extends Evolving[AddedThenRenamed] { val evolutions = Evolutions[AddedThenRenamed] }

  val upperCase: String => String = _.toUpperCase

  /* TransformedThenRenamed at version 0, then at version 1
  case class TransformedThenRenamed(sessionId: String)
  @version(1) case class TransformedThenRenamed(@transformed(since = 1, upperCase) sessionId: String)
   */

  @version(2)
  case class TransformedThenRenamed(@transformed(since = 1, upperCase) @renamed(since = 2, "sessionId") id: String)
  object TransformedThenRenamed extends Evolving[TransformedThenRenamed] {
    val evolutions = Evolutions[TransformedThenRenamed]
  }

  /* RenamedOverDeleted and RenamedOverDeclaredDeleted at version 0
  case class RenamedOverDeleted(a: Int, b: Int)
   */

  @version(1)
  case class RenamedOverDeleted(@renamed(since = 1, "b") a: Int)
  object RenamedOverDeleted extends Evolving[RenamedOverDeleted] { val evolutions = Evolutions[RenamedOverDeleted] }

  @version(1)
  @deletedFields(since = 1, "a")
  case class RenamedOverDeclaredDeleted(@renamed(since = 1, "b") a: Int)
  object RenamedOverDeclaredDeleted extends Evolving[RenamedOverDeclaredDeleted] {
    val evolutions = Evolutions[RenamedOverDeclaredDeleted]
  }

}
