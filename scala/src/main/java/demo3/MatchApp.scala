package demo3

import scala.util.Random

/**
  * 模式匹配
  */
object MatchApp {
  def main(args: Array[String]): Unit = {
    val names = Array("zs", "ls", "ww")
    val name = names(Random.nextInt(names.length))

    name match {
      case "zs" =>
        print("zs被选中")
      case "ls" =>
        print("ls被选中")
      case "w" =>
        print("ww被选中")
      case _ =>
        print("没人被选中")
    }

//    judgeGrade("wsfdf", "D")

    // 数组匹配

    typeMatch(1)
    typeMatch("1")

  }

  def judgeGrade(name: String, grade: String): Unit = {
    grade match {
      case "A" =>
        print("A成绩")
      case "B" =>
        print("A成绩")
      case "C" =>
        print("A成绩")
      // 双重过滤
      case _ if name.equals("ls") =>
        print(name + "无成绩")
      case _ =>
        print("无成绩")
    }
  }

  def arrayMatch(array: Array[String]): Unit = {
    array match {
      case Array("zs") =>
        print("zs")
      case Array(x, y) =>
        print("x=" + x + "-------y=" + y)
      case Array("zs", _*) =>
        print("zs and others")
      case _ =>
        print("no medthod")
    }
  }

  def listMatch(list: List[String]): Unit = {
    list match {
      case "zs" :: Nil => print("zs")
      case x :: y :: Nil => print("x=" + x + "---y=" + y)
      case "zs" :: tail => print("zs and others")
      case _ => print("nil")
    }
  }

  def typeMatch(obj: Any): Unit = {
    obj match {
      case x: Int => print("Int")
      case x: String => print("String")
      case x: Map[_, _] => print("Map")
      case _ => print("Nil")
    }
  }
}
