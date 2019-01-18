package demo3

object CaseClassApp {
  def main(args: Array[String]): Unit = {
    casePersion(CTO("zs"))
    casePersion(EMP("ls"))
  }

  def casePersion(persion: Persion): Unit = {
    persion match {
      case CTO(name: String) => println("CTO name:" + name)
      case EMP(name: String) => println("EMP name:" + name)
    }
  }
}

class Persion

case class CTO(name: String) extends Persion

case class EMP(name: String) extends Persion
