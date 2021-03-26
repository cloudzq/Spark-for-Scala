//代码文件为/usr/local/scala/mycode/TestMatch.scala
import scala.io.StdIn._
println("Please input the score:")
val grade=readChar()
grade match{
	case 'A' => println("85-100")
	case 'B' => println("70-84")
	case 'C' => println("60-69")
	case 'D' => println("<60")
	case _ => println("error input!")
}
