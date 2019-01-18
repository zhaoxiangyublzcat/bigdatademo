package sparkstreaming.class1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用SparkStreaming处理文件系统（lcoal/hdfs）数据
  */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.textFileStream("file:///Users/blz/test/")
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
