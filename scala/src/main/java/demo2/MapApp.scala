package demo2

/**
  * map集合操作
  */
object MapApp {
  def main(args: Array[String]): Unit = {
    // 1.
    val a = Map(
      "red" -> "red",
      "black" -> "black"
    )
    // 反回了map key的迭代器
    a.keys
    // 反回了map value的迭代器
    a.values
    // 判断是否为空
    a.isEmpty

    val b = Map(
      "green" -> "green",
      "white" -> "white"
    )

    // ++ 作为运算符
    a ++ b
    //++ 作为方法
    a.++(b)
    // 2.
    val c = scala.collection.mutable.Map[String, Int]()
    c += ("数据1" -> 1)

    // 遍历
    c.keys.foreach(i => print(i))
    // 是否存在某个键
    c.contains("a")
  }
}
