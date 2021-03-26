//代码文件为/usr/local/scala/mycode/Element.scala
abstract class Element{
type T //抽象的类型成员
	   var value:T //抽象的字段，类型为T
	   def show:Unit //抽象方法，需要根据具体的类型T进行实现
}
class IntEle(var value:Int) extends Element{
type T = Int
	   def show{printf("My value is %d.\n",value)} //T是Int型时的输出
}
class StringEle(var value:String) extends Element{
type T = String
	   def show{printf("My value is %s.\n",value)}//T是String型时的输出
}
