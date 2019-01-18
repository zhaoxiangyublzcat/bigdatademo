package demo4

object StringApp {
  def main(args: Array[String]): Unit = {
    // 1. 单行
    var s1 = "我是被拼接的字符串"
    // 插值----字符串拼接s"$被拼接部分"
    val a = s"我是字符串1$s1"
    print(a) // 我是字符串1我是被拼接的字符串
    // 2. 多行
    var s2 =
      """
        |这是多
        |行
        |字符串
      """.stripMargin
    print(s2)
  }
}
