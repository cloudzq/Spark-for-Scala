import org.apache.spark.{Partitioner, SparkContext, SparkConf}
//�Զ�������࣬��Ҫ�̳�org.apache.spark.Partitioner��
class MyPartitioner(numParts:Int) extends Partitioner{
  //���Ƿ�����
  override def numPartitions: Int = numParts  
  //���Ƿ����Ż�ȡ����
  override def getPartition(key: Any): Int = {
    key.toString.toInt%10
}
}
object TestPartitioner {
  def main(args: Array[String]) {
    val conf=new SparkConf()
    val sc=new SparkContext(conf)
    //ģ��5������������
    val data=sc.parallelize(1 to 10,5)
    //����β��ת��Ϊ10���������ֱ�д��10���ļ�
    data.map((_,1)).partitionBy(new MyPartitioner(10)).map(_._1).saveAsTextFile("file:///usr/local/spark/mycode/rdd/partitioner")
  }
} 

