//代码文件为/usr/local/scala/mycode/TestMatch3.scala
for (elem <- List(1,2,3,4)){
elem match {
   case _ if (elem%2==0) => println(elem + " is even.")
case _ => println(elem + " is odd.")
}
}
