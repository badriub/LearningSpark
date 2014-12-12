package bm.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.hypertable.hadoop.mapreduce.InputFormat;
import org.hypertable.hadoop.mapreduce.KeyWritable;

import bm.util.RDDUtility;

public class HypertableConnect {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = RDDUtility.createSparkContext();
		
		Configuration configuration = new Configuration();
//	    configuration.addResource(new Path("file:///opt/hypertable/current/conf/hypertable.cfg"));
	    configuration.set(InputFormat.TABLE, "Pages");

	    JavaPairRDD<KeyWritable, BytesWritable> data = javaSparkContext.newAPIHadoopRDD(configuration, 
	    		InputFormat.class,KeyWritable.class, BytesWritable.class);
	    
	    System.out.println(data.count());
	}

}
