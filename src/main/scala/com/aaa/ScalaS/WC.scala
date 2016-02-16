package com.aaa.ScalaS


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WC {
    def main(args: Array[String]) {
      
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      
      val lines = sc.textFile("/user/cloudera/wc/inp")

      val words = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey( (x,y) => x+y)

      words.collect().foreach(println)
      
    }
}
 