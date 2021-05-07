package com.abc.spark.test

import com.abc.spark.AnonDemo
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}


class AnonDemoTest extends FlatSpec with BeforeAndAfter {
  // SparkContext Ref can be used in all tests
  var sc: SparkContext = _

  // Test 1
  it should "return an empty array" in {
    val seq = Seq()
    val rdd: RDD[String] = sc.parallelize(seq)
    val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
    val arr: Array[String] = anonRDD.collect()
    assert(arr.length == 0)
  }

  // Test 2
  it should "return a empty string" in {
    val seq = Seq("aaa,bbb,ccc,ddd,eee")
    val rdd: RDD[String] = sc.parallelize(seq)
    val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
    val arr: Array[String] = anonRDD.collect()
    assert(arr(0).equals(""))
  }

  // Test 3
  it should "test success" in {
    val seq = Seq(
      "Lily,Chan,15 Hillside Drive Campbelltown SA 5078,1995-10-11",
      "Cherry,Zhang,16 Hillside Drive Campbelltown SA 5078,2000-12-22"
    )
    val rdd: RDD[String] = sc.parallelize(seq)
    val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
    val arr: Array[String] = anonRDD.collect()
    assert(arr(0).equals("********,********,********,1995-10-11"))
    assert(arr(1).equals("********,********,********,2000-12-22"))
  }

  // Test 4
  it should "write the anonymised data to specified directory" in {
    val rdd: RDD[String] = sc.textFile("E:\\temp\\data\\data.csv")
    val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
    anonRDD.saveAsTextFile("E:\\temp\\data\\output")
  }

  // Test 5
  it should "throw a FileAlreadyExistsException" in {
    assertThrows[FileAlreadyExistsException] {
      val rdd: RDD[String] = sc.textFile("E:\\temp\\data\\data.csv")
      val anonRDD: RDD[String] = AnonDemo.anonymise(rdd)
      anonRDD.saveAsTextFile("E:\\temp\\data\\output")
    }
  }

  // Create SparkContext before tests
  before {
    val conf: SparkConf = new SparkConf().setAppName("AnonDemoTest").setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  // Close SparkContext after tests
  after {
    if (sc != null) {
      sc.stop()
    }
  }
}

