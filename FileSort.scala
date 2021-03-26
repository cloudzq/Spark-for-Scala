import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
object FileSort {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("FileSort")
        val sc = new SparkContext(conf)
        val dataFile = "file:///usr/local/spark/mycode/rdd/data"
        val lines = sc.textFile(dataFile,3)
        var index = 0
        val result = lines.filter(_.trim().length>0).map(n=>(n.trim.toInt,"")).partitionBy(new HashPartitioner(1)).sortByKey().map(t => {
　　　　　　index += 1
            (index,t._1)
        })
        result.saveAsTextFile("file:///usrl/local/spark/mycode/rdd/examples/result")
    }
}
