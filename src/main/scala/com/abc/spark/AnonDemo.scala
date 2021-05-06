package com.abc.spark


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AnonDemo {

  def main(args: Array[String]): Unit = {
    // 1. Create SparkConf
    val conf: SparkConf = new SparkConf().setAppName("AnonDemo")

    // 2. Create SparkContext
    val sc = new SparkContext(conf)

    // 3. Read data file from arg1
    val linesRDD: RDD[String] = sc.textFile(args(1))

    // 4. Anonymise columns
    val anonRDD: RDD[String] = anonymise(linesRDD)

    // 5. Write context of rdd to specified directory
    anonRDD.saveAsTextFile(args(2))

    // 6. Close SparkContext
    sc.stop()
  }


  /**
    * @param rdd whick need to be anonymised
    * @return an anonymised rdd
    */
  def anonymise(rdd: RDD[String]): RDD[String] = {
    rdd.map(_.split(",")).map {
      case Array(fn, ln, addr, bd) => "********,********,********," + bd
      case _ => ""
    }
  }

}
