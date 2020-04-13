import org.apache.spark.sql.SparkSession
import java.lang.Math
object Main{
  private val SYS_PATH="src/main/resources/"
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("quick_start").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc=spark.sparkContext
    val data=sc.textFile(SYS_PATH+"data.csv")

//    print("--------------"+data.first())
    print(data.count())
    val lineWithSpark=data.filter(_.contains("1"))
//    transform
//    print(lineWithSpark.first()+"-------------------1")
    val count1=data.filter(_.contains("2")).count()
//    transform and action
//    print(count1+"----------------------------------2")
//    mapreduce reduce 调用reduce left ，从左开始合并，两两调用方法
    val mapReduce=data.map(_.split("1").size).reduce((a,b)=>if(a>b) a else b)
//    print(mapReduce+"-------------------------------3")
//    方法可以调用java方法 看着对迭代的效果不错
    val MathReduce=data.map(_.split("1").size).reduce((a,b)=>Math.max(a,b))
//    print(MathReduce+"-------------------------4")
    val mapReduceDemo=data.flatMap(_.split("1")).groupBy(identity).count()
//    print(mapReduceDemo+"-------------------------5")
//    当大量重复查询少量数据或者进行迭代算法时，将数据提取到集群范围的内存中缓存。
    val cachData=data.map(_.split("1")).cache()
//    print(cachData.count())
    spark.stop()
  }
}