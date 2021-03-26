package org.apache.spark.examples.streaming
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaWordCount{
def main(args:Array[String]){
StreamingExamples.setStreamingLogLevels()
val sc = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
val ssc = new StreamingContext(sc,Seconds(10))
ssc.checkpoint("file:///usr/local/spark/mycode/kafka/checkpoint") //���ü��㣬��������HDFS���棬��д������ssc.checkpoint("/user/hadoop/checkpoint")������ʽ�����ǣ�Ҫ����hadoop
val zkQuorum = "localhost:2181" //Zookeeper��������ַ
val group = "1"  //topic���ڵ�group����������Ϊ�Լ���Ҫ�����ƣ����粻��1������val group = "test-consumer-group" 
val topics = "wordsender"  //topics������
val numThreads = 1  //ÿ��topic�ķ�����
val topicMap =topics.split(",").map((_,numThreads.toInt)).toMap
val lineMap = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
val lines = lineMap.map(_._2)
val words = lines.flatMap(_.split(" "))
val pair = words.map(x => (x,1))
val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2) //���д���ĺ�������һ�ڵĴ���ת�������л��н���
wordCounts.print
ssc.start
ssc.awaitTermination
}
}
