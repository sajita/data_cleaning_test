import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


class ploTest {

  def logExample(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    import org.apache.spark.sql.functions.split


    val spark = SparkSession.builder()
      .appName("WebLog")
      .master("local[*]")
      .getOrCreate()
    //this will produce a dataframe with a single column called value
    import spark.implicits._

    val base_df = spark.read
      //.option("delimiter", "\t")
      .option("header", false)
      .text("/home/sajita/Desktop/postgresql-10-main.log")


    val splitDF = base_df.withColumn("date", split($"value", "\\+").getItem(0))
      //.withColumn("time", split($"value", "\\s+").getItem(1))
      //.withColumn("utc+", split($"value", "\\s+").getItem(2))
      .withColumn("pid", split($"value", "\\s+").getItem(3))
      .withColumn("log", split($"value", "\\s+").getItem(4))
      .withColumn("message", split($"value", "\\s+").getItem(5))
      .withColumn("pid", regexp_replace($"pid", "[\\[\\]]", " " +
        ""))
      .withColumn("timestamp", date_format(to_utc_timestamp(to_timestamp(
        $"date", "yyyy-MM-dd HH:mm:ss.SSS"), "Asia/Kathmandu"), "yyyy-MM-dd HH:mm:ss"))
      //.withColumn("timestamp", array("date", "time", "utc+"))
      .drop($"value")
    splitDF.show(truncate = false)
    //splitDF.printSchema()

  }
}

object Plog {
  def main(args: Array[String]): Unit = {
    val log = new ploTest
    log.logExample()
  }
}