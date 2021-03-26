import org.apache.spark.SparkContext 
import org.apache.spark.SparkConf 
object Combine {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Combine").setMaster(¡°local¡±)
        val sc = new SparkContext(conf)
        val data = sc.parallelize(Array(("company-1",88),("company-1",96),("company-1",85),("company-2",94),("company-2",86),("company-2",74),("company-3",86),("company-3",88),("company-3",92)),3)
        val res = data.combineByKey(
            (income) => (income,1),
            ( acc:(Int,Int), income ) => ( acc._1+income, acc._2+1 ),
            ( acc1:(Int,Int), acc2:(Int,Int) ) => ( acc1._1+acc2._1, acc1._2+acc2._2 )
        ).map({ case (key, value) => (key, value._1, value._1/value._2.toFloat) })
        res.repartition(1).saveAsTextFile("file:///usr/local/spark/mycode/rdd/result")
    }
}
