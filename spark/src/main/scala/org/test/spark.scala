package org.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._

import org.apache.spark.streaming.kafka._
import org.apache.spark._

//import kafka.serializer.Decoder
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka.kafkautils
//import kafka.serializer._



object spark {
  
  def main(args: Array[String])
  {
    
    val conf = new SparkConf().setAppName("KafkaSpark").setMaster("local")
 val sc = new StreamingContext(conf, Seconds(2))

 //val test = sc.textFile("food.txt")
 val test = sc.sparkContext.textFile("food.txt", 1)
 println("TEST APPLI SCALA")
 test.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).saveAsTextFile("food.count.txt")
 
    /* val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    
        // Create direct kafka stream with brokers and topics
    //val topics = "test4"
    //val topicsSet = topics.split(",").toSet
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)


    //val numThreads = args
    
val kafkaParams = Map(
  "metadata.broker.list" -> "localhost:9093",
  "zookeeper.connect" -> "localhost:2181",
  "group.id" -> "kafka-spark-streaming-example",
  "zookeeper.connection.timeout.ms" -> "1000")
  
  val kafkaConf = Map(
    "metadata.broker.list" -> "localhost:9092", // Default kafka broker list location
    "zookeeper.connect" -> "localhost:2181", // Default zookeeper location
    "group.id" -> "kafka-spark-streaming-example",
    "zookeeper.connection.timeout.ms" -> "1000")

  // Create a new stream which can decode byte arrays.  For this exercise, the incoming stream only contain user and product Ids

    val topics = "test4"
    val numThreads = 1
    val topicpMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val zkQuorum = "localhost:2181"
    val lines = KafkaUtils.createStream(ssc, zkQuorum, "consumer-group", topicpMap)
      .map { case (key, value) => ((key, Math.floor(System.currentTimeMillis() / 60000).toLong * 60), value.toInt) }

      ssc.start
      
      println("DEMARRAGE JOB SPARK STREAMING KAFKA")

    ssc.awaitTermination
    
    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sc, kafkaParams, topicsSet)
    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sc, kafkaParams, "test4")

     //val lines = messages.map(_._2)
    //val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //wordCounts.print()
     *
     */
  }
}
  