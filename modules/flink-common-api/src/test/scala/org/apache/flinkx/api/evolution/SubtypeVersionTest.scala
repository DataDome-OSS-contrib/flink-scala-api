package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.CoproductSerializer.CoproductSerializerSnapshot
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The data of a subtype has the version of the subtype, not the version of its sealed trait: the `since` of its rename
  * or deletion counts in its own versions.
  */
class SubtypeVersionTest extends AnyFlatSpec with Matchers with TestUtils {

  import SubtypeVersionTest._

  it should "restore a renamed case class under a sealed trait of a higher version" in {
    val formerSerializer = formerLitter

    deserializeFormerForm[Litter](formerSerializer, FormerCaseClass("Rex")) shouldBe Dog("Rex")
    compatibilityWith[Litter](formerSerializer)(infoToSer[Litter]) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "restore a renamed case object under a sealed trait of a higher version" in {
    deserializeFormerForm[Litter](formerLitter, FormerCaseObject) shouldBe Wanderer
  }

  it should "restore a deleted unversioned subtype under a sealed trait of a higher version" in {
    deserializeFormerForm[Litter](formerLitter, FormerRunt("Tiny")) shouldBe null
  }

  // Herd keeps its version: the rename of its subtype counts only in the versions of the subtype
  it should "restore a renamed subtype under an unchanged sealed trait" in {
    val foalName         = classOf[Colt].getName.replace("Colt", "Foal")
    val formerSerializer =
      formerTrait(classOf[Herd], 1, (foalName, classOf[FormerCaseClass], formerCaseClass(foalName, 0)))

    deserializeFormerForm[Herd](formerSerializer, FormerCaseClass("Spirit")) shouldBe Colt("Spirit")
    compatibilityWith[Herd](formerSerializer)(infoToSer[Herd]) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "restore a renamed subtype of a version higher than the one of its sealed trait" in {
    val lambName         = classOf[Sheep].getName.replace("Sheep", "Lamb")
    val formerSerializer =
      formerTrait(classOf[Flock], 1, (lambName, classOf[FormerCaseClass], formerCaseClass(lambName, 3)))

    deserializeFormerForm[Flock](formerSerializer, FormerCaseClass("Dolly")) shouldBe Sheep("Dolly")
    compatibilityWith[Flock](formerSerializer)(infoToSer[Flock]) shouldBe Symbol("compatibleAfterMigration")
  }

  // Ram had the version 2: its deletion counts in its own versions, above the version of Flock
  it should "restore a deleted versioned subtype" in {
    val ramName          = classOf[Sheep].getName.replace("Sheep", "Ram")
    val formerSerializer =
      formerTrait(classOf[Flock], 1, (ramName, classOf[FormerCaseClass], formerCaseClass(ramName, 2)))

    deserializeFormerForm[Flock](formerSerializer, FormerCaseClass("Aries")) shouldBe null
  }

  // Before release 2.4.0, the snapshot format 2 records the subtype names, then the snapshots of the subtypes
  it should "restore a renamed subtype from a snapshot of format 2" in {
    val puppyName = classOf[Dog].getName.replace("Dog", "Puppy")
    val puppy     = new CaseClassSerializer[Product](
      new AdtEvolution(puppyName, 0, classOf[Product]),
      isCaseClassImmutable = true,
      fieldNames = None,
      paramSerializers = Array(StringSerializer.INSTANCE)
    )
    val out = new DataOutputSerializer(1024)
    out.writeUTF(classOf[CoproductSerializerSnapshot[_]].getName)
    out.writeInt(2) // Snapshot format version
    out.writeInt(1) // Number of subtypes
    out.writeUTF(puppyName)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, puppy.snapshotConfiguration())
    out.writeByte(0) // Index of the subtype
    puppy.serialize(FormerCaseClass("Rex"), out)

    val in       = new DataInputDeserializer(out.getCopyOfBuffer)
    val snapshot = TypeSerializerSnapshot.readVersionedSnapshot[Litter](in, getClass.getClassLoader)
    snapshot.restoreSerializer().deserialize(in) shouldBe Dog("Rex")
  }

  // The Seq records the name of Stray, which refers to Wanderer at version 0, and to Stray at version 2
  it should "deserialize a Seq of a case object that reuses the former name of a renamed case object" in {
    roundtrip[Seq[Stray.type]](infoToSer[Seq[Stray.type]], Seq(Stray))
  }

  it should "not compile a rename of a subtype since a version above its own version" in {
    assertTypeError("Evolutions[WrongRenamedSinceAboveOwnVersion]")
  }

  it should "tell the last version that the evolutions declare for a former class name" in {
    val puppyName = classOf[Dog].getName.replace("Dog", "Puppy")
    val exception = intercept[MissingEvolutionException](Evolutions.get(puppyName, 1, getClass.getClassLoader))
    exception.getMessage shouldBe s"Cannot restore '$puppyName' written at version 1: the evolutions declare this class" +
      " name up to version 0. Declare its @renamed or @deletedClasses since a version above 1"
  }

  /** A former serializer of Litter at version 2. The stand-in classes take the place of its former subtypes. */
  private def formerLitter: TypeSerializer[AnyRef] = {
    val puppyName  = classOf[Dog].getName.replace("Dog", "Puppy")
    val strayName  = classOf[Dog].getName.replace("Dog", "Stray$")
    val runtName   = classOf[Dog].getName.replace("Dog", "Runt")
    val caseObject = new ScalaCaseObjectSerializer[AnyRef](
      new AdtEvolution(strayName, 0, FormerCaseObject.getClass.asInstanceOf[Class[AnyRef]])
    )
    formerTrait(
      classOf[Litter],
      2,
      (puppyName, classOf[FormerCaseClass], formerCaseClass(puppyName, 0)),
      (strayName, FormerCaseObject.getClass, caseObject),
      (runtName, classOf[FormerRunt], formerCaseClass(runtName, 0))
    )
  }

  /** A former serializer of the sealed trait `traitClass` at `traitVersion`, with its former subtypes: their former
    * class name, the stand-in class that takes their place, and their serializer.
    */
  private def formerTrait(
      traitClass: Class[_],
      traitVersion: Int,
      subtypes: (String, Class[_], TypeSerializer[_])*
  ): TypeSerializer[AnyRef] =
    new CoproductSerializer[AnyRef](
      evolution = Some(evolutionOf(traitClass, traitVersion).asInstanceOf[AdtEvolution[AnyRef]]),
      subtypeClasses = subtypes.map(_._2).toArray,
      subtypeNames = subtypes.map(_._1).toArray,
      subtypeSerializers = subtypes.map(_._3).toArray
    )

  /** The serializer of a former case class with one String field, written at `version` under `formerClassName`. */
  private def formerCaseClass(formerClassName: String, version: Int): TypeSerializer[_] =
    new CaseClassSerializer[Product](
      new AdtEvolution(formerClassName, version, classOf[Product]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("name")),
      paramSerializers = Array(StringSerializer.INSTANCE)
    )

}

object SubtypeVersionTest {

  /* Litter at version 2
  @version(2)
  sealed trait Litter
  case class Puppy(name: String) extends Litter
  case object Stray extends Litter
  case class Runt(name: String) extends Litter
   */

  @version(3)
  @deletedClasses(since = 1, throwOnInstance = false, "Runt")
  sealed trait Litter
  object Litter extends Evolving[Litter] { val evolutions = Evolutions[Litter] }
  @version(1)
  @renamed(since = 1, "Puppy")
  case class Dog(name: String) extends Litter
  object Dog                   extends Evolving[Dog] { val evolutions = Evolutions[Dog] }
  @version(1)
  @renamed(since = 1, "Stray")
  case object Wanderer extends Litter
  @version(2)
  case object Stray extends Litter

  /* Herd at version 1
  @version(1)
  sealed trait Herd
  case class Foal(name: String) extends Herd
   */

  @version(1)
  sealed trait Herd
  object Herd extends Evolving[Herd] { val evolutions = Evolutions[Herd] }
  @version(1)
  @renamed(since = 1, "Foal")
  case class Colt(name: String) extends Herd
  object Colt                   extends Evolving[Colt] { val evolutions = Evolutions[Colt] }

  /* Flock at version 1
  @version(1)
  sealed trait Flock
  @version(3)
  case class Lamb(name: String) extends Flock
  @version(2)
  case class Ram(name: String) extends Flock
   */

  @version(1)
  @deletedClasses(since = 3, throwOnInstance = false, "Ram")
  sealed trait Flock
  object Flock extends Evolving[Flock] { val evolutions = Evolutions[Flock] }
  @version(4)
  @renamed(since = 4, "Lamb")
  case class Sheep(name: String) extends Flock
  object Sheep                   extends Evolving[Sheep] { val evolutions = Evolutions[Sheep] }

  @version(2)
  sealed trait WrongParent
  @version(1)
  @renamed(since = 2, "Former")
  case class WrongRenamedSinceAboveOwnVersion(name: String) extends WrongParent

  // Take the place of the former subtypes, with other names to avoid a name reuse
  case class FormerCaseClass(name: String)
  case class FormerRunt(name: String)
  case object FormerCaseObject

}
