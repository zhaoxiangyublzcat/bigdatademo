package sparkstreaming.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming对接kafka
  */
object KafkaReceiverWordCount {
  val zkQuorum = "192.168.80.139:2181"
  val group = "biogroup_1"
  val topics = "biotopic"
  val numThreads = "1"

  def main(args: Array[String]): Unit = {

    val sc = new SparkConf()
//      .setAppName("KafkaReceiverWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sc, Seconds(5))

    val topicsMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // SparkStreaming对接kafka核心部分
    val message = KafkaUtils.createStream(ssc, zkQuorum, group, topicsMap)
    message.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
