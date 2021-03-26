import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.parsing.json.JSON
object JSONRead {
    def main(args: Array[String]) {
        val inputFile = ¡°file:///usr/local/spark/examples/src/main/resources/people.json"
        val conf = new SparkConf().setAppName("JSONRead")
        val sc = new SparkContext(conf)
        val jsonStrs = sc.textFile(inputFile)
        val result = jsonStrs.map(s => JSON.parseFull(s))
        result.foreach( {r => r match {
                        case Some(map: Map[String, Any]) => println(map)
                        case None => println("Parsing failed")
                        case other => println("Unknown data structure: " + other)
                }
        }
        )
    }
}
