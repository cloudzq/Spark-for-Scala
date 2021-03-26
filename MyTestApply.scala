//代码文件为/usr/local/scala/mycode/MyTestApply.scala
class Car(name: String) {
    def info() {
        println("Car name is "+ name)
    }
}
object Car {
    def apply(name: String) = new Car(name) //调用伴生类Car的构造方法
}
object MyTestApply{
    def main (args: Array[String]) {
    val mycar = Car("BMW") //调用伴生对象中的apply方法		 mycar.info() //输出结果为“Car name is BMW”
    }
}
