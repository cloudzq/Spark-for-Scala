import org.apache.spark._
import org.apache.spark.streaming._
object WordCountStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[2]")  //设置为本地运行模式，2个线程，一个监听，另一个处理数据
    val ssc = new StreamingContext(sparkConf, Seconds(2))   // 时间间隔为2秒
    val lines = ssc.textFileStream("file:///usr/local/spark/mycode/streaming/logfile")  //这里采用本地文件，当然也可以采用HDFS文件
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
