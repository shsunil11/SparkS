package com.aaa.JavaS;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.Durations;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.flume.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

//JavaReceiverInputDStream<SparkFlumeEvent>flumeStream =
//  FlumeUtils.createPollingStream(streamingContext, [sink machine hostname], [sink port]);

public class StreamJNRT {

	public static void main(String args[]) throws Exception {

		SparkConf  conf = new SparkConf();

		// Create a StreamingContext with a 1-second batch size from a SparkConf
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10)); 
		
		// Create a DStream from all the input on port 7777
		JavaReceiverInputDStream<SparkFlumeEvent> events = FlumeUtils
				.createPollingStream(jssc, "localhost", 7777);

		// Convert SparkFlumeEvent to String
		JavaDStream<String> eventStr = events
				.map(new Function<SparkFlumeEvent, String>() {
					public String call(SparkFlumeEvent e) {
						return new String(e.event().getBody().array());

					}
				});

		eventStr.print(100);
		
		// Write batch to HDFS and print batch to screen
		eventStr.foreachRDD(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> rdd) {
				rdd.saveAsTextFile("/user/cloudera/spk/nrt_"
						+ System.currentTimeMillis());
				int i = 0;
				for (String event : rdd.collect()) {
					System.out.println("Event No = " + (++i) + " " + event);
				}
				return null;
			}
		});

		// Write batch to HBase table "spk"
		eventStr.foreachRDD(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> rdd) {

				Configuration conf1 = HBaseConfiguration.create();
				conf1.set(TableInputFormat.INPUT_TABLE, "spk");
				conf1.set("hbase.zookeeper.quorum", "localhost:2181");
				
				Job jconf = null;
				try {
				   jconf = Job.getInstance(conf1);
				} catch (Exception e) {
			      e.printStackTrace();
		       }

				jconf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "spk");
				jconf.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
				// Convert to PairRDD before writing to HBase
				JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
							//@Override
							public Tuple2<ImmutableBytesWritable, Put> call(
									String row) throws Exception {

								Put put = new Put(Bytes.toBytes(row
										.split("\\|")[0]));
								put.addColumn(Bytes.toBytes("cf1"),
										Bytes.toBytes("col1"),
										Bytes.toBytes(row.split("\\|")[1]));

								put.addColumn(Bytes.toBytes("cf1"),
										Bytes.toBytes("col2"),
										Bytes.toBytes(row.split("\\|")[2]));

								
								return new Tuple2<ImmutableBytesWritable, Put>(
										new ImmutableBytesWritable(), put);
							}
						});
                hbasePuts.saveAsNewAPIHadoopDataset(jconf.getConfiguration());
				return null;
			}
		});

		// Start our streaming context and wait for it to "finish"
		jssc.start();
		// Wait for the job to finish
		jssc.awaitTermination();
	}

}
