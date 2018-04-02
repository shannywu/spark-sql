import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SparkSession

object Csv2Parquet {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("CSV to Parquet")
      .getOrCreate()

    transform(spark)
  }

  private def transform(spark: SparkSession): Unit = {
    import spark.implicits._

    val path = "src/main/resources/people.csv"
    val peopleDF = spark
      .read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .csv(path)

    peopleDF.write.parquet("people_csv.parquet")

    val peopleParquetDF = spark.read.parquet("people_csv.parquet")
    peopleParquetDF.show()
    // +-----+----+---------+
    // | name| age|      job|
    // +-----+----+---------+
    // |Jorge|  30|Developer|
    // |  Bob|  32|Developer|
    // |Angel|null|  Teacher|
    // +-----+----+---------+
  }

}