//代码文件为/usr/local/scala/mycode/Bird3.scala
class Animal(val category:String){
    def info(){println("This is a "+category)}
}
trait HasLegs {
		val legs:Int   //抽象字段
     def move(){printf("I can walk with %d legs",legs)}
   }
