//代码文件为/usr/local/scala/mycode/Person.scala
object Person {
    private var lastId = 0  //一个人的身份编号
    def newPersonId() = {
        lastId +=1
        lastId
    }
}
