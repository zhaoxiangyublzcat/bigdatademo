package sparkstreaming.class1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * DStrem与RDD操作  - 黑名单过滤
  */
object TransFormApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("TransFormApp")
    val ssc = new StreamingContext(sc, Seconds(5))

    // 构建黑名单
    val blacksRDD = ssc.sparkContext.parallelize(List("zs", "ls")).map(x => (x, true))
    val lines = ssc.socketTextStream("localhost", 6789)
    val res = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD).filter(x => !x._2._2.getOrElse(false)).map(x => x._2._1)
    })

    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
