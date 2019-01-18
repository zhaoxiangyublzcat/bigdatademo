package sparkstreaming.class1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 累计算子stateful - 统计从开始到现在所有
  */
object StateWordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("StateWordCount")
    val ssc = new StreamingContext(sc, Seconds(5))
    // 使用stateful算子，必须设置checkpoint
    // 生产环境中把checkpoint设置到hdfs某一个文件夹中
    ssc.checkpoint("./")

    val lines = ssc.socketTextStream("localhost", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1))
    val status = result.updateStateByKey(updateFunc)
    status.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 把当前的数据更新到老数据中
    *
    * @param currentValues 当前数据
    * @param preValues     已有的数据
    * @return
    */
  def updateFunc(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val newCount = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(newCount + pre)
  }


}
