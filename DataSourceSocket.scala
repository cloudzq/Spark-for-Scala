package org.apache.spark.examples.streaming
import  java.io.{PrintWriter}
import  java.net.ServerSocket 
import  scala.io.Source
object DataSourceSocket {
  def index(length: Int) = { //返回位于0到length-1之间的一个随机数
    val rdm = new java.util.Random
    rdm.nextInt(length)
  }
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }
    val fileName = args(0)  //获取文件路径
    val lines = Source.fromFile(fileName).getLines.toList  //读取文件中的所有行的内容
    val rowCount = lines.length  //计算出文件的行数
val listener = new ServerSocket(args(1).toInt)  //创建监听特定端口的ServerSocket对象
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(2).toLong)  //每隔多长时间发送一次数据
            val content = lines(index(rowCount))  //从lines列表中取出一个元素
            println(content)
            out.write(content + '\n')  //写入要发送给客户端的数据
            out.flush()  //发送数据给客户端
          }
          socket.close()
        }
      }.start()
    }
  }
}
