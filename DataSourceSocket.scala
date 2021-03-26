package org.apache.spark.examples.streaming
import  java.io.{PrintWriter}
import  java.net.ServerSocket 
import  scala.io.Source
object DataSourceSocket {
  def index(length: Int) = { //����λ��0��length-1֮���һ�������
    val rdm = new java.util.Random
    rdm.nextInt(length)
  }
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }
    val fileName = args(0)  //��ȡ�ļ�·��
    val lines = Source.fromFile(fileName).getLines.toList  //��ȡ�ļ��е������е�����
    val rowCount = lines.length  //������ļ�������
val listener = new ServerSocket(args(1).toInt)  //���������ض��˿ڵ�ServerSocket����
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(2).toLong)  //ÿ���೤ʱ�䷢��һ������
            val content = lines(index(rowCount))  //��lines�б���ȡ��һ��Ԫ��
            println(content)
            out.write(content + '\n')  //д��Ҫ���͸��ͻ��˵�����
            out.flush()  //�������ݸ��ͻ���
          }
          socket.close()
        }
      }.start()
    }
  }
}
