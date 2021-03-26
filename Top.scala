//代码文件为/usr/local/scala/mycode/Top.scala
class  Top(name:String,subname:String){ //顶层类
case class Nested(name:String) //嵌套类
def show{
val c=new Nested(subname)
printf("Top %s includes a Nested %s\n",name,c.name)
}
}
val t = new Top("A","B")
t.show
