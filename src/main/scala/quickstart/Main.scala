import org.apache.spark.sql.SparkSession

object Main{
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("quick_start").getOrCreate()
    val sc=spark.sparkContext
    var data=sc.textFile("data.csv")
  }
}