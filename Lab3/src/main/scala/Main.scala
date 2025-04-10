import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Reports with Apache Spark")
      .master("local[*]")
      .getOrCreate()

    val programmingLanguagesDataFrame: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/programming-languages.csv")

    val programmingLanguages = programmingLanguagesDataFrame.select("name")
      .rdd
      .map(lang => lang(0))
      .map(lang => lang.toString)
      .map(lang => lang.toLowerCase)
      .collect()
      .toList

    programmingLanguages.take(10).foreach(println)

    val posts = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .option("inferSchema", value = true)
      .load("data/posts_sample.xml")

    val yearLanguageTags = posts.rdd
      .map(row => (row(6), row(18)))
      .filter(row => row._1 != null & row._2 != null)
      .map(row => (row._1.toString, row._2.toString))
      .map(row => (row._1.substring(0, 4), row._2.split(">")))
      .flatMap {
        case (row) => row._2.map(tag => (row._1, tag.replace("<", "")))
      }
      .filter(row => programmingLanguages.contains(row._2))

    val years = 2010 to 2020 map(_.toString)

    val report = years.map { reportYear =>
      yearLanguageTags.filter {
        row => row._1 == reportYear
      }.map {
        row => (row._2, 1)
      }.reduceByKey(_ + _)
        .map(row => (reportYear, row._1, row._2))
        .sortBy(row => row._3, ascending = false)
        .take(10)
        .toList
    }.toList

    val finalReport = report.reduce((a, b) => a.union(b))

    val df = spark.createDataFrame(finalReport).select(col("_1").alias("Year"),
      col("_2").alias("Language"),
      col("_3").alias("Count"))

    df.show()

    df.write.parquet("report")

    spark.stop()
  }
}
