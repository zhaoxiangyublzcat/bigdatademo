package demo

/**
  * 抽象类
  */
object AbstractApp {
  def main(args: Array[String]): Unit = {
    val s1 = new Student()
    s1.speak()
  }
}

abstract class Persion {
  val name: String
  val age: Int

  def speak()
}

class Student extends Persion {
  override val name: String = null
  override val age: Int = 0

  override def speak(): Unit = {
    print("I'm" + name)
  }
}
