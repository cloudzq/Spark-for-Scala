//代码文件为/usr/local/scala/mycode/Box.scala
import scala.collection.mutable.Stack
class Box[T]{
	val elems:Stack[T]=Stack()
	def remove:Option[T]={ //返回的对象采用了Option类型进行包装
		if (elems.isEmpty) None else Some(elems.pop) 
	}
	def append(a1:T){elems.push(a1)}
}
