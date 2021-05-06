package com.abc.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

//引入scalatest建立一个单元测试类，混入特质BeforeAndAfter，在before和after中分别初始化sc和停止sc，
//初始化SparkContext时只需将Master设置为local(local[N],N表示线程)即可，无需本地配置或搭建集群，

class AnonDemoTest extends FlatSpec with BeforeAndAfter {
  var sc: SparkContext = _

  it should "test success 1" in {
    //其中参数为rdd或者dataframe可以通过通过简单的手动构造即可
    val seq = Seq()
    val rdd: RDD[String] = sc.parallelize(seq)
    val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
    anonRDD.collect().foreach(println)
  }

  it should "test success 2" in {
    //其中参数为rdd或者dataframe可以通过通过简单的手动构造即可
    val seq = Seq("Lily,Chan,15 Hillside Drive Campbelltown SA 5078,1995-10-11")
    val rdd: RDD[String] = sc.parallelize(seq)
    val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
    anonRDD.collect().foreach(println)
  }

  //it should "test success 3" in {
  //  //其中参数为rdd或者dataframe可以通过通过简单的手动构造即可
  //  val rdd: RDD[String] = sc.textFile("E:\\temp\\data\\data.csv")
  //  val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
  //  anonRDD.saveAsTextFile("E:\\temp\\data\\output")
  //}

  //这里before和after中分别进行sparkcontext的初始化和结束，如果是SQLContext也可以在这里面初始化
  before {
    val conf: SparkConf = new SparkConf().setAppName("AnonDemoTest").setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}

