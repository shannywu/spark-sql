import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession


object Json2Parquet {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("JSON to Parquet")
      .getOrCreate()

    runTransformation(spark)
  }

  def assertSameSize(arrs:Seq[_]*) = {
    assert(arrs.map(_.size).distinct.size==1,"sizes differ")
  }

  private def runTransformation(spark: SparkSession): Unit = {
    import spark.implicits._

    val multi_zip = udf((
        name:Seq[String], hobbies:Seq[Seq[String]], age:Seq[String]
      ) => {
        assertSameSize(name, hobbies, age)
        name.indices.map(i => (name(i), hobbies(i), age(i)))
      }
    )
    
    // read a JSON file with object occupies multiple lines
    val path = "src/main/resources/people.json"
    val peopleDF = spark.read.option("multiline", "true").json(path)

    peopleDF.show()

    val transPeopleDF = peopleDF
      .withColumn(
        "cols", explode(
          multi_zip($"result.name", $"result.hobbies", $"result.age")
        )
      )
      .select(
        $"cols._1".alias("name"), $"cols._2".alias("hobbies"), $"cols._3".alias("age"),
        $"latest_update_date"
      )

    transPeopleDF.write.parquet("people.parquet")

    val peopleParquetDF = spark.read.parquet("people.parquet")

    peopleParquetDF.show()
  }
}