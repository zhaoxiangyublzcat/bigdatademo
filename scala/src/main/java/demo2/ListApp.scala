package demo2

object ListApp {
  def main(args: Array[String]): Unit = {
    // 1. Nil  List(null)
    // 2.
    val a = List(1, 2, 3, 4)
    print(a.head)
    println()
    print(a.tail)

    // 2. 1
    var b = 1 :: Nil
    // 2 1
    b = 2 :: b
    b.length
    // 3.
    val c = scala.collection.mutable.ListBuffer(1, 2, 3)
    c += 4
    c += (5, 6)
    c ++= List(7, 8, 9)

    c -= 4
    c -= (4, 5, 6)
    c --= List(1, 2, 3)
    c.toList
    c.toArray
    c.isEmpty
    c.head
    c.tail
  }
}
