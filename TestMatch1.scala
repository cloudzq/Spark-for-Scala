//代码文件为/usr/local/scala/mycode/TestMatch1.scala
import scala.io.StdIn._
println("Please input a country:")
val country=readLine()
country match{
		case "China" => println("中国")
		case "America" => println("美国")
		case "Japan" => println("日本")
		case _ => println("我不认识!")
}
