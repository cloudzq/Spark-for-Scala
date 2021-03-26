//�����ļ�ΪInsertStudent.scala
import java.util.Properties 
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
 
//���������������ݣ���ʾ����ѧ������Ϣ
val studentRDD = spark.sparkContext.parallelize(Array("3 Rongcheng M 26","4 Guanhua M 27")).map(_.split(" "))
 
//��������ģʽ��Ϣ
val schema = StructType(List(StructField("id", IntegerType, true),StructField("name", StringType, true),StructField("gender", StringType, true),StructField("age", IntegerType, true)))
 
//���洴��Row����ÿ��Row������rowRDD�е�һ��
val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
 
//������Row�����ģʽ֮��Ķ�Ӧ��ϵ��Ҳ���ǰ����ݺ�ģʽ��Ӧ����
val studentDF = spark.createDataFrame(rowRDD, schema)
 
//���洴��һ��prop������������JDBC���Ӳ���
val prop = new Properties()
prop.put("user", "root") //��ʾ�û�����root
prop.put("password", "hadoop") //��ʾ������hadoop
prop.put("driver","com.mysql.jdbc.Driver") //��ʾ����������com.mysql.jdbc.Driver
 
//�����������ݿ⣬����appendģʽ����ʾ׷�Ӽ�¼�����ݿ�spark��student����
studentDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark","spark.student",prop)
