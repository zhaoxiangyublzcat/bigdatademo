package demo4

object HighFunctionApp {
  def main(args: Array[String]): Unit = {
    val l = List(1, 2, 3, 4, 5, 6, 7, 8)
    // .map：逐渐操作集合中的每个元素
    l.map((x: Int) => x + 1)
    l.map((x) => x + 1)
    l.map(x => x + 1)
    l.map(_ + 1)
    // .filter 过滤元素
    l.map(_ + 1).filter(_ > 3)
    l.take(3)

    // == l.sum
    l.reduce(_ + _)
    // 从左往右依次相减
    l.reduceLeft(_ - _)
    // 从右往左依次相减
    l.reduceRight(_ - _)
    //
//    l.fold(_ - _)
    // 从左往右依次相减
//    l.foldLeft(_ - _)
    //
//    l.foldRight(_ - _)

    l.max
    l.min
    l.sum

    val l2 = List(List(1, 2), List(3, 4), List(5, 6))
    // 合并成一个list
    l2.flatten
    // === flatten + map
    l2.flatMap(_.map(_ + 1))
  }
}
