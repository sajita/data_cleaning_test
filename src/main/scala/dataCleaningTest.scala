import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
class dataCleaningTest {
  def CleanExample(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Spark Postgres")
      .getOrCreate()

    val dataDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/sajita/Desktop/data_test.csv")
    dataDF.withColumn("salary", dataDF("id")+dataDF("name")+dataDF("address")+dataDF("age")).show(false)


    //dataDF.show()

  }
}
object testData {
  def main(args: Array[String]) {
    val addCol = new dataCleaningTest
    addCol.CleanExample()
  }
}
