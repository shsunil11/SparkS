package com.aaa.ScalaS

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client._;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

object StreamSNRT {
  
  def main(args: Array[String]) {
     val batchInterval = Milliseconds(10000)
    
     val sparkConf = new SparkConf().setAppName("FlumePollingEventCount")
     val ssc = new StreamingContext(sparkConf, batchInterval)
     val flumeStream = FlumeUtils.createPollingStream(ssc, "localhost", 7777)
     val strStream = flumeStream.map(e => new String(e.event.getBody().array()))
     
     // Print out the count of events received from this server in each batch
    strStream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    strStream.foreachRDD({rdd => rdd.collect().foreach(println) })
    
    strStream.saveAsTextFiles("/user/cloudera/spks", "txt")
    
    strStream.foreachRDD(rdd=> {
       val hconf = HBaseConfiguration.create()
			 //hconf.set(TableInputFormat.INPUT_TABLE, "spks")
			 hconf.set("hbase.zookeeper.quorum", "localhost:2181")
			 hconf.set(TableOutputFormat.OUTPUT_TABLE, "spks")
		
			 val jobConf = Job.getInstance(hconf)	
       //jobConf.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "spks")
			 jobConf.setOutputFormatClass(classOf[TableOutputFormat[String]])
			 
			// setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class)
       
       val hbasePuts = rdd.map(row => {
         val put = new Put(Bytes.toBytes(row.split("\\|")(0)))
										
								put.addColumn(Bytes.toBytes("cf1"),
										Bytes.toBytes("col1"),
										Bytes.toBytes(row.split("\\|")(1)))

								put.addColumn(Bytes.toBytes("cf1"),
										Bytes.toBytes("col2"),
										Bytes.toBytes(row.split("\\|")(2)))
										
								(new ImmutableBytesWritable(), put)
       })
       hbasePuts.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  
}