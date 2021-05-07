package com.abc.spark


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AnonDemo {

  def main(args: Array[String]): Unit = {
    // 1. Receive parameters
    var input: String = null
    var output: String = null
    if(args.length != 2) {
      throw new Exception("Illegal number of arguments")
    } else {
      input = args(0)
      output = args(1)
    }

    // 2. Create SparkConf
    val conf: SparkConf = new SparkConf().setAppName("AnonDemo")

    // 3. Create SparkContext
    val sc = new SparkContext(conf)

    // 4. Read data file from arg1
    val linesRDD: RDD[String] = sc.textFile(input)

    // 5. Anonymise columns
    val anonRDD: RDD[String] = anonymise(linesRDD)

    // 6. Write context of rdd to specified directory
    anonRDD.saveAsTextFile(output)

    // 7. Close SparkContext
    sc.stop()
  }


  /**
    * @param rdd whick need to be anonymised
    * @return an anonymised rdd
    */
  def anonymise(rdd: RDD[String]): RDD[String] = {
    rdd.map(_.split(",")).map {
      case Array(fn, ln, addr, bd) => "********,********,********," + bd
      case _ => ""  // The blank lines will be filted in DWD
    }
  }

}
