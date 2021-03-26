//代码文件为/usr/local/scala/mycode/MyCar.scala
abstract class Car(val name:String) {
	   val carBrand:String //一个抽象字段
var age:Int=0
    def info() //抽象方法
    def greeting() {
        println("Welcome to my car!")
    }
    def this(name:String,age:Int) {
        this(name)
        this.age=age
    }
}
//派生类，其主构造函数调用了父类的主构造函数
// 由于name是父类主构造器的参数，因此也必须override
class BMWCar(override val name:String) extends Car(name) {
    override val carBrand = "BMW"  //重载父类抽象字段，override可选
    def info() {//重载父类抽象方法，override关键字可选
        printf("This is a %s car. It has been used for %d year.\n", carBrand,age)
    }
  override def greeting() {//重载父类非抽象方法，override必选
        println("Welcome to my BMW car!")
    }
}
//派生类，其主构造函数调用了父类的辅助构造函数
class BYDCar(name:String,age:Int) extends Car(name,age) {
    val carBrand = "BYD" //重载父类抽象字段，override可选
    override def info() {//重载父类抽象方法，override关键字可选
        printf("This is a %s car.It has been used for %d year.\n", carBrand,age)
    }
}
object MyCar{
    def main(args:Array[String]) {
        val car1 = new BMWCar("Bob's Car")
        val car2 = new BYDCar("Tom's Car",3)
        show(car1)
        show(car2)
}
//将参数设为父类类型，根据传入参数的具体子类类型，调用相应方法，实现多态
    def show(thecar:Car)={thecar.greeting; thecar.info()}
}
