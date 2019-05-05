package rdd

import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test1").setMaster("local[5]"))
    val inputRDD = sc.textFile("hdfs://bio.cloudera.com:8020/tmp/test1.txt")
    inputRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(print(_))
  }
}
