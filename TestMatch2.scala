//代码文件为/usr/local/scala/mycode/TestMatch2.scala
for (elem <- List(6,9,0.618,"Spark","Hadoop",'Hello)){
	val str  = elem match  {
	    case i: Int => i + " is an int value."//匹配整型的值，并赋值给i
	    case d: Double => d + " is a double value." //匹配浮点型的值
		case "Spark"=>"Spark is found." //匹配特定的字符串
		case s: String => s + " is a string value." //匹配其它字符串
		case _ =>"unexpected value："+ elem  //与以上都不匹配
}
	  println(str)
}
