package org.apache.flinkx.api.serializer

import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.serializer.Scala3EnumValueSerializerSnapshotTest.Failure
import org.apache.flinkx.api.serializer.SnapshotTestUtil.savepointed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Scala3EnumValueSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the same enum value is serialized" in {
    val registeredSnapshot = enumValueSerializer("MISSING_KEY").snapshotConfiguration()
    val restoredSnapshot   = savepointed[Failure](enumValueSerializer("MISSING_KEY"))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when another enum value is serialized" in {
    val registeredSnapshot = enumValueSerializer("MISSING_KEY").snapshotConfiguration()
    val restoredSnapshot   = savepointed[Failure](enumValueSerializer("PARSE_ERROR"))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  private def enumValueSerializer(enumValueName: String) =
    new Scala3EnumValueSerializer[Failure](evolutionOf(classOf[Failure]), enumValueName)

}

object Scala3EnumValueSerializerSnapshotTest {

  enum Failure {
    case MISSING_KEY, PARSE_ERROR
  }

}
