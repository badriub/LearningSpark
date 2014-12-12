package bm.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaHBaseLoad {
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
		sparkConf.setMaster("local");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		String tableName = "t1";
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf
				.addResource(new Path("file:///opt/hbase/conf/hbase-site.xml"));
		hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);

		JavaPairRDD<ImmutableBytesWritable, Result> data = sparkContext
				.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
						ImmutableBytesWritable.class, Result.class);
		
		System.out.println(data.count());
	}
}
