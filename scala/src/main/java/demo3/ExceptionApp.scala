package demo3

/**
  * 异常处理
  */
object ExceptionApp {
  def main(args: Array[String]): Unit = {
    try {
      print(1 / 0)
    } catch {
      case e: ArithmeticException => print("除数不能为0")
      case e: Exception => print(e.getMessage)
    }finally {
      // 释放资源
    }
  }
}
