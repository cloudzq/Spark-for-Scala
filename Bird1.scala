//代码文件为/usr/local/scala/mycode/Bird1.scala
trait Flyable {
		var maxFlyHeight:Int  //抽象字段
     def fly() //抽象方法
def breathe(){ //具体的方法
         println("I can breathe")
     }
 }
class Animal(val category:String){
    def info(){println("This is a "+category)}
}
class Bird(flyHeight:Int) extends Animal("Bird") with Flyable{
var maxFlyHeight:Int = flyHeight //重载特质的抽象字段
     def fly(){
printf("I can fly at the height of %d",maxFlyHeight)
}//重载特质的抽象方法
}
