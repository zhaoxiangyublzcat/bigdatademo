package sparkstreaming.class1

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming完成词频统计,并将数据添加到mysql数据库
  */
object MysqlWordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("StateWordCount")
    val ssc = new StreamingContext(sc, Seconds(5))
    // 使用stateful算子，必须设置checkpoint
    // 生产环境中把checkpoint设置到hdfs某一个文件夹中
    ssc.checkpoint("./")

    val lines = ssc.socketTextStream("localhost", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    // *******************结果入库
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount values('" + record._1 + "', " + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取mysql链接
    *
    * @return conn
    */
  def createConnection(): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3307/test", "root", "root")
  }
}
