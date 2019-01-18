package demo

/**
  * 伴生类伴生对象
  */
object ApplyApp {
  def main(args: Array[String]): Unit = {
    //    val a = ApplyDemo()
    val a1 = new ApplyDemo()
    a1()
    //    for (i <- 1 to 10) {
    //      a.add()
    //    }
    //    print(a.sum)
  }
}

/**
  * 伴生类
  */
object ApplyDemo {
  var sum = 0

  def add(): Unit = {
    sum += 1
  }

  def apply(): ApplyDemo = {
    print("调用了object apply 方法")
    new ApplyDemo
  }
}

/**
  * 伴生对象
  */
class ApplyDemo {
  def apply(): Unit = {
    print("调用了class apply 方法")
  }
}

