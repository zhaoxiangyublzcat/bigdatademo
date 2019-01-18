package demo4

object CurryingApp {
  def main(args: Array[String]): Unit = {
    println(sum(1)(2))
  }

  def sum(num1: Int)(num2: Int): Int ={
    num1 + num2
  }
}
