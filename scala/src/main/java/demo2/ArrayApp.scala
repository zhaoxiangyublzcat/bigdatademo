package demo2

/**
  * 数组
  */
object ArrayApp {
  def main(args: Array[String]): Unit = {
    // 1 定长度
    val a = new Array[String](5)
    print(a.length)
    a(0) = "hello"
    // 2 object中apply方法 定长度
    val b = Array(1, 2, 3, 4)
    b.sum
    b.max
    b.min
    b.mkString(",")
    //3 可变长度
    val c = scala.collection.mutable.ArrayBuffer(1, 2, 3)
    c += 4
    c += (5, 6, 7, 8)
    c ++= Array(9, 10, 11, 12)
    c.insert(1, 100)
    c.remove(1)
    c.trimStart(1)
    c.trimEnd(1)
    c.toArray
    for (i <- c) {
      print(i)
    }
  }
}
