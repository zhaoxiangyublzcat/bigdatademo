//package scala.demo5
//
//object Test1 {
//  implicit def object2SpecialPersion(obj: Object): SpecialPersion = {
//    if (obj.getClass == classOf[Sutdent]) {
//      val stu = obj.asInstanceOf[Sutdent]
//      new SpecialPersion(stu.name)
//    } else if (obj.getClass == classOf[OldPersion]) {
//      val old = obj.asInstanceOf[OldPersion]
//      new SpecialPersion(old.name)
//    } else {
//      Nil
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//
//  }
//}
//
//class SpecialPersion(val name: String)
//
//class Sutdent(val name: String)
//
//class OldPersion(val name: String)
//
