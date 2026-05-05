package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.AnyValEncoderSpec._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class AnyValEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers
    with TypedEncoderImplicits {

  implicit val simpleTypeEncoder: TypedEncoder[Foo] =
    TypedEncoder.xmap[Foo, String] { s =>
      s.value
    }(Foo.apply)

  "AnyValEncoder" when {
    "used with AnyVal" should {

      "work with value class in case class" in {

        // Value classes as fields in case classes (erased at JVM level)
        Bar("1", Foo("Hello!"), 10) should haveTypedEncoder[Bar]()
        Baz(Foo("Hello!")) should haveTypedEncoder[Baz]()
      }


      "work with top-level value class" in {
        // Top-level Dataset[Foo] should work - the schema is the underlying type (String)
        TypedEncoder[Foo].catalystRepr shouldBe StringType

        val ds = spark.createDataset(Seq(Foo("Hello!")))
        ds.schema shouldBe StructType(Seq(StructField("value", StringType, nullable = true)))
        ds.collect().head shouldBe Foo("Hello!")

        // Test that a raw thing would work too
        val ds2 = spark.createDataset(Seq("Hello!")).as[Foo]
        ds2.collect().head shouldBe Foo("Hello!")

        Foo("Hello!") should haveTypedEncoder[Foo]()
      }

      "work within a dataset" in {
        val schema = StructType(Seq(
          StructField("id", StringType, nullable = false),
          StructField("key", StringType, nullable = false),
          StructField("value", IntegerType, nullable = false)))

        TypedEncoder[Bar].catalystRepr shouldBe schema

        val ds = spark.createDataset(Seq(Bar("1", Foo("Hello!"), 10)))
        ds.collect().head shouldBe Bar("1", Foo("Hello!"), 10)
      }

      "work with arrays" in {
        val ds = spark.createDataset(Seq(IndexedSeq(Foo("Hello!"), Foo("World!"))))
        ds.schema shouldBe StructType(Seq(StructField(
          "value",
          ArrayType(StringType, containsNull = false),
          nullable = false)))
        ds.collect().head shouldBe Array(Foo("Hello!"), Foo("World!"))
      }

      "work with more arrays" in {
        val basicData = Seq(Seq(Bar("1", Foo("Hello!"), 11), Bar("2", Foo("Hello2!"), 12)))
        val ds = spark.createDataset(basicData)
        ds.collect().head shouldBe Array(Bar("1", Foo("Hello!"), 11), Bar("2", Foo("Hello2!"), 12))
      }

      "work with invariants (runtime detection)" in {
        // xmap auto-detects value classes at runtime
        implicit val simpleTypeEncoder: TypedEncoder[SimpleTypeStr] =
          TypedEncoder.xmap[SimpleTypeStr, String] { s =>
            s.value
          }(SimpleTypeStr(_))

        implicit val typedEncoder: TypedEncoder[ContainedSimple] =
          TypedEncoder.derive[ContainedSimple]

        ContainedSimple(SimpleTypeStr("true"), 10) should haveTypedEncoder[ContainedSimple]()

        val ds = spark.createDataset(Seq(SimpleTypeStr("true")))
        ds.collect().head shouldBe SimpleTypeStr("true")

        val ds2 = spark.createDataset(Seq(ContainedSimple(SimpleTypeStr("true"), 10)))
        ds2.collect().head shouldBe ContainedSimple(SimpleTypeStr("true"), 10)
      }

      "work with invariants (compile-time verified)" in {
        // xmapValueClass verifies at compile time that the type is a value class
        implicit val simpleTypeEncoder: TypedEncoder[SimpleTypeStr] =
          TypedEncoder.xmap[SimpleTypeStr, String] { s =>
            s.value
          }(SimpleTypeStr(_))

        implicit val typedEncoder: TypedEncoder[ContainedSimple] =
          TypedEncoder.derive[ContainedSimple]

        ContainedSimple(SimpleTypeStr("true"), 10) should haveTypedEncoder[ContainedSimple]()

        val ds = spark.createDataset(Seq(SimpleTypeStr("true")))
        ds.collect().head shouldBe SimpleTypeStr("true")

        val ds2 = spark.createDataset(Seq(ContainedSimple(SimpleTypeStr("true"), 10)))
        ds2.collect().head shouldBe ContainedSimple(SimpleTypeStr("true"), 10)
      }

      "preserve non-anyval" in {
        implicit val simpleTypeEncoder: TypedEncoder[NonAnyVal] =
          TypedEncoder.xmap[NonAnyVal, String] { s =>
            s.value
          }(NonAnyVal(_))

        NonAnyVal("Hello!") should haveTypedEncoder[NonAnyVal]()
      }

      "work with type holes" in {

        implicit val simpleTypeEncoder: TypedEncoder[SimpleTypeStr] =
          TypedEncoder.xmap[SimpleTypeStr, String] { s =>
            s.value
          }(SimpleTypeStr(_))

        Test("1", SimpleTypeStr("Hello!")) should haveTypedEncoder[Test[SimpleTypeStr]]()
      }

      "read nullable nested array structs with AnyVal fields" in {
        val sourceSchema = StructType(Seq(
          StructField("key", StringType, nullable = false),
          StructField(
            "updates",
            ArrayType(
              StructType(Seq(
                StructField("lineIndex", IntegerType, nullable = false),
                StructField("sourceBillId", StringType, nullable = true))),
              containsNull = false),
            nullable = false)))

        val rows = spark.sparkContext.parallelize(Seq(
          Row("policy-1", Seq(Row(0, "bill-1"), Row(1, "bill-2")))))

        val df = spark.createDataFrame(rows, sourceSchema)
        val result = df.as[NestedRepairGroup[Foo]].collect().toSeq

        result shouldBe Seq(NestedRepairGroup(
          Foo("policy-1"),
          List(
            NestedLineUpdate(0, Foo("bill-1")),
            NestedLineUpdate(1, Foo("bill-2")))))
      }
    }
  }
}

object AnyValEncoderSpec {
  case class Foo(value: String) extends AnyVal

  case class Bar(id: String, key: Foo, value: Int)

  case class Baz(foo: Foo)

  case class SimpleTypeStr(value: String) extends AnyVal

  case class ContainedSimple(simple: SimpleTypeStr, bar: Int)

  case class NonAnyVal(value: String)

  case class Test[K](id: String, key: K)

  case class NestedLineUpdate(lineIndex: Int, sourceBillId: Foo)

  case class NestedRepairGroup[K](key: K, updates: List[NestedLineUpdate])
}
