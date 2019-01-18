package demo

object CaseClassApp {
  def main(args: Array[String]): Unit = {
    println(Persion("测试").name)
  }
}

case class Persion(name: String)
