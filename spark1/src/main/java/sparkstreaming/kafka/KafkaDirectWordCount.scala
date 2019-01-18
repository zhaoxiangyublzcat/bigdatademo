package sparkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming + Kafak（direct）
  * 常用
  */
object KafkaDirectWordCount {
  val topics = "biotopic"
  val brokes = "192.168.80.139:6667"

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
    //      .setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokes)
    val topicsSet = topics.split(",").toSet
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    message.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
