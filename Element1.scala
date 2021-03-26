//代码文件为/usr/local/scala/mycode/Element1.scala
abstract class Element[T]{
var value:T
	  def show:Unit
}
class IntEle(var value:Int) extends Element[Int]{
	  def show{printf("My value is %d.\n",value)}
}
class StringEle(var value:String) extends Element[String]{
	  def show{printf("My value is %s.\n",value)}
}
