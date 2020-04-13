package rdd_programming_guide

import org.apache.spark.sql.SparkSession
class Main {

}
object Main{
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]").appName("rdd").getOrCreate()
    
  }
}
