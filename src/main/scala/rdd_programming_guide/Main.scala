package rdd_programming_guide

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
class Main {

}
object Main{
  def main(args: Array[String]): Unit = {
//    val spark=SparkSession.builder().master("local[*]").appName("rdd").getOrCreate()
//    每一个应用程序会包含一个驱动程序，就是driver，driver 去运行该用户的main方法并再集群上执行
//    spark 上两种特色，一种是rdd，是一种分布式弹性数据集，弹性是指定大小是弹性的，就是一个rdd可以是多行或者一行这种。
//    再就是共享变量，共享变量分两种，一种广播变量(在所有节点上的内存中缓存值)和累加器(计数器和sum这些)
//    spark 程序做的第一件事就是告诉spark 如何访问集群，所以要建一个sparkContext，对sparkcontext 里面其实都是配置项，也是上下文
//    所以你要向sparkcontext 里面加入配置项，比如访问集群的类型，方法等等，配置这些要生成一个sparkconf对象。
//    使用spark-shell --master 指定master ，以及核数，jar包等都可以指定。
//    还可以设置maven依赖指定
    val conf=new SparkConf().setAppName("rdd_programming").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
//    设置将数据生成未分布式数据集,第二个参数是设置分片数，每一个分片创建一个task。
//    这个主要是并行计算用，就是把一个任务划分为多个task时候用的
    val data=Array(1,2,3,4,5,6)
//    val distData=sc.parallelize(data)
//    print(distData)
//    rdd操作包括transform和action，spark是懒加载，只记录transform，当到达下一个是action时才会计算action，把结果传回来。
//    并且这样比较节省，只把结果传给驱动这样比较省内存。
//    collect 数据的所有元素作为一个数组返回到driver，数据量大了会把driver整挂
//    可以用以下进行分区排序
//    mapPartitions 使用例如，对每个分区进行排序 .sorted
//    repartitionAndSortWithinPartitions 在对分区进行有效排序的同时进行重新分区
//      sortBy 制作全局排序的RDD
//    会引起shuffle的一般就是repartition，coalesce等重分区，以及groub by key ，reduce by key 以及join操作 cogroup的。
//    一般删除缓存使用最近最少使用的方式丢弃就的数据分区。如果像手动删除，就rdd.unpersist()
//    广播变量是只读的
//    spark 的action执行时根据stage来的，这些stage根据shuffle分开，spark自动广播每个stage需要的是通用之前，在每个任务运行之前
//    会将数据以序列话的形式缓存并反序列化。
//    这意味着只有当多个stage需要同一份数据的时候广播变量才有用。
//    val broadcatVar=sc.broadcast(Array(1,2,3))
//    print(broadcatVar.value)
//    累加器，只有加的操作，可以实现计数，累加器可以在集群上运行，但是只有driver才能调用获取累加器的值。
//    累加器保证每个任务对累加器更新只有一次，重启任务不会更新。累加器也不会改变图，只有当它时作为rdd计算的一部分时才会跟新，在惰性转换的时候不能保证更新。
    val accum=sc.longAccumulator("Accumulator test")
    sc.parallelize(Array(1,2,4,5)).foreach(accum.add(_))
    println(accum)
//    var counter=0
//    var rdd=sc.parallelize(data)
//    rdd.map(counter+=_)
//    println("counter value:",counter)



    
  }
}
