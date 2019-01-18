package demo2

object SetApp {
  def main(args: Array[String]): Unit = {
    // 1
    val a = Set(1, 2, 3)
    // 2
    val b = scala.collection.mutable.Set[Int]()
    b += 1
    b += (1, 2, 3)
    b ++= Set(4, 5, 6)
  }
}
