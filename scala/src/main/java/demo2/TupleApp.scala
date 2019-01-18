package demo2

/**
  * tuple
  */
object TupleApp {
  def main(args: Array[String]): Unit = {
    // 1.
    val t1 = (1, 3.14, "Fred")
    // 2.
    val t2 = new Tuple3(1, 3.14, "Fred")
    // 访问元组
    val visitT = t1._2
    // 迭代元组
    t1.productIterator.foreach { i => println("Value = " + i) }
    // 元组转换成字符串
    t1.toString
    // 元素交换
//    t1.swap
  }
}
