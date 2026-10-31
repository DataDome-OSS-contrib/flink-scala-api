package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.serializer.{CaseClassSerializer, Scala3EnumSerializer, Scala3EnumValueSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The ADT evolution of a Scala 3 enum, which holds the evolutions of its enum values. */
class EvolutionMacroEnumTest extends AnyFlatSpec with Matchers with TestUtils {

  import org.apache.flinkx.api.Scala3EnumTest.FailureCategory
  import EvolutionMacroEnumTest.*

  // A deleted value can have parameters, and then a class of its own in each name of its enum
  it should "declare the enum, the former name it was renamed from and the class names of its deleted value" in {
    val formerName = classOf[FailureCategory].getName.replace("FailureCategory", "FailureType")
    FailureCategory.evolutions.declarations.map(_.className).toSet shouldBe Set(
      classOf[FailureCategory].getName,
      formerName,
      s"${classOf[FailureCategory].getName}$$OTHER_TYPE",
      s"$formerName$$OTHER_TYPE"
    )
  }

  // The ADT evolution of the enum holds the deletion and the rename of its values: a value has no ADT evolution
  it should "declare the evolutions of the enum values" in {
    val evolution = evolutionOf(classOf[FailureCategory])

    evolution.currentEnumValueName("MISSING_TYPE") shouldBe Some("MISSING")
    evolution.currentEnumValueName("OTHER_TYPE") shouldBe None
    intercept[DeletedInstanceException](evolution.migrateEnumValue("OTHER_TYPE")(_ => fail("A deleted value is read")))
  }

  // A value added in version 1 reuses the former name of a renamed value
  it should "apply the evolution of an enum value only to the data written before it" in {
    evolutionOf(classOf[Letter]).currentEnumValueName("A") shouldBe Some("B")
    evolutionOf(classOf[Letter], 1).currentEnumValueName("A") shouldBe Some("A")
  }

  it should "serialize an enum value that reuses the former name of a renamed value as itself" in {
    val serializer = infoToSer[Letter]
    val out        = new DataOutputSerializer(64)
    serializer.serialize(Letter.A, out)

    serializer.deserialize(new DataInputDeserializer(out.getSharedBuffer)) shouldBe Letter.A
    roundtrip(serializer, Letter.A) // Read again by the restored serializer
  }

  // The two values exchange their names: the names stay the same, but the migration is necessary
  it should "migrate the values of an enum swapping their names" in {
    given TypeSerializer[Swapped] = infoToSer[Swapped]
    val v0                        = evolutionOf(classOf[Swapped])
    val formerSerializer          = new Scala3EnumSerializer[Swapped](
      evolution = Some(v0),
      enumValueNames = Array("A", "B"),
      enumValueSerializers =
        Array[TypeSerializer[?]](new Scala3EnumValueSerializer(v0, "A"), new Scala3EnumValueSerializer(v0, "B"))
    )

    deserializeFormerForm[Swapped](formerSerializer, Swapped.A) shouldBe Swapped.B
    compatibilityWith[Swapped](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "not compile a declaration renaming an enum value since a version above the one of its enum" in {
    assertTypeError("Evolutions[WrongEnumValueRenamedSinceAboveVersion]")
  }

  // An enum value with parameters has a class of its own, whose snapshot records its class name and its fields
  it should "migrate an enum value with parameters renamed with an added field" in {
    given TypeSerializer[Vehicle] = infoToSer[Vehicle]
    val formerSerializer          = formerVehicleSerializer(classOf[Vehicle].getName)

    deserializeFormerForm[Vehicle](formerSerializer, FormerValue.Car(2)) shouldBe Vehicle.Auto(2, 4)
    compatibilityWith[Vehicle](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "migrate a deleted enum value with parameters to null" in {
    deserializeFormerForm[Vehicle](formerVehicleSerializer(classOf[Vehicle].getName), FormerValue.Truck(9)) shouldBe
      null
  }

  // The class name of an enum value with parameters contains the name of its enum. A rename alone reads as is
  it should "migrate an enum value with parameters of a renamed enum" in {
    given TypeSerializer[Fleet] = infoToSer[Fleet]
    val navyName                = classOf[Fleet].getName.replace("Fleet", "Navy")
    val formerSerializer        = new Scala3EnumSerializer[Product](
      evolution = Some(Evolutions.get(navyName, 0, getClass.getClassLoader)),
      enumValueNames = Array("Ship"),
      enumValueSerializers = Array[TypeSerializer[?]](formerValueSerializer(s"$navyName$$Ship", "crew"))
    )

    deserializeFormerForm[Fleet](formerSerializer, FormerValue.Ship(3)) shouldBe Fleet.Ship(3)
    compatibilityWith[Fleet](formerSerializer) shouldBe Symbol("compatibleAsIs")
  }

  it should "plan the migration of a generic enum value with parameters" in {
    evolutionOf(classOf[Box.Full[?]]).plan(Array("a")).map(_.length) shouldBe Right(2)
  }

  it should "not compile a declaration adding a field without default value to an enum value" in {
    assertTypeError("Evolutions[WrongEnumValueAddedWithoutDefault]")
  }

  // Without an evolving companion, only the derivation of the enum can reject the annotations of its values
  it should "reject the derivation of an enum without version that declares an evolution of a value" in {
    assertTypeError(
      "{ import org.apache.flinkx.api.auto._; deriveTypeInformation[WrongEnumValueRenamedWithoutVersion] }"
    )
  }

  /** The serializer of Vehicle at version 0: FormerValue takes the place of its values with parameters. */
  private def formerVehicleSerializer(vehicleName: String): Scala3EnumSerializer[Product] =
    new Scala3EnumSerializer[Product](
      evolution = Some(evolutionOf(classOf[Vehicle]).asInstanceOf[AdtEvolution[Product]]),
      enumValueNames = Array("Car", "Truck", "Bike"),
      enumValueSerializers = Array[TypeSerializer[?]](
        formerValueSerializer(s"$vehicleName$$Car", "seats"),
        formerValueSerializer(s"$vehicleName$$Truck", "load"),
        new Scala3EnumValueSerializer(evolutionOf(classOf[Vehicle]), "Bike")
      )
    )

  /** The serializer of a former enum value with one Int parameter, whose snapshot records `formerClassName`. */
  private def formerValueSerializer(formerClassName: String, field: String): TypeSerializer[?] =
    new CaseClassSerializer[FormerValue](
      evolution = new AdtEvolution(formerClassName, 0, classOf[FormerValue]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array(field)),
      paramSerializers = Array(IntSerializer.INSTANCE)
    )

}

object EvolutionMacroEnumTest {

  /* Letter at version 0
  enum Letter { case A }
   */

  @version(1)
  enum Letter {
    @renamed(since = 1, "A") case B
    case A
  }
  object Letter extends Evolving[Letter] { val evolutions = Evolutions[Letter] }

  /* Swapped at version 0
  enum Swapped { case A, B }
   */

  @version(1)
  enum Swapped {
    @renamed(since = 1, "B") case A
    @renamed(since = 1, "A") case B
  }
  object Swapped extends Evolving[Swapped] { val evolutions = Evolutions[Swapped] }

  enum WrongEnumValueRenamedWithoutVersion {
    @renamed(since = 1, "A") case B
  }

  @version(1)
  enum WrongEnumValueRenamedSinceAboveVersion {
    @renamed(since = 2, "A") case B
  }

  /* Vehicle at version 0
  enum Vehicle { case Car(seats: Int); case Truck(load: Int); case Bike }
   */

  @version(1)
  @deletedClasses(since = 1, throwOnInstance = false, "Truck")
  enum Vehicle {
    @renamed(since = 1, "Car") case Auto(seats: Int, @added(since = 1) doors: Int = 4)
    case Bike
  }
  object Vehicle extends Evolving[Vehicle] { val evolutions = Evolutions[Vehicle] }

  // Takes the place of the former values with parameters, with other names to avoid a name reuse
  enum FormerValue {
    case Car(seats: Int)
    case Truck(load: Int)
    case Ship(crew: Int)
  }

  /* Fleet at version 0
  enum Navy { case Ship(crew: Int) }
   */

  @version(1)
  @renamed(since = 1, "Navy")
  enum Fleet { case Ship(crew: Int) }
  object Fleet extends Evolving[Fleet] { val evolutions = Evolutions[Fleet] }

  /* Box at version 0
  enum Box[+A] { case Full(a: A); case Empty }
   */

  @version(1)
  enum Box[+A] {
    case Full(a: A, @added(since = 1) n: Int = 0)
    case Empty
  }
  object Box extends Evolving[Box[?]] { val evolutions = Evolutions[Box[?]] }

  @version(1)
  enum WrongEnumValueAddedWithoutDefault { case A(@added(since = 1) a: Int) }

}
