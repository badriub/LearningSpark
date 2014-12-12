package bm.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import scala.Tuple2;
import bm.model.SpendDataset;
import bm.util.RDDUtility;

public class HbaseConnect {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
		sparkConf.setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		String tableName = "t1";
		Configuration hbaseConf = HBaseConfiguration.create();
	    hbaseConf.addResource(new Path("file:///opt/hbase/conf/hbase-site.xml"));
	    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
	    hbaseConf.setInt("timeout", 30);
	    
	    System.out.println("51");
	    JavaPairRDD<ImmutableBytesWritable, Result> data = sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,ImmutableBytesWritable.class, Result.class);
	    
	    JavaRDD<SpendDataset> sd = data.map(new Function<Tuple2<ImmutableBytesWritable,Result>, SpendDataset>() {

			public SpendDataset call(Tuple2<ImmutableBytesWritable, Result> tuple)
					throws Exception {
				System.out.println(tuple);
				Result result = tuple._2();
				
				SpendDataset spendDataset = new SpendDataset();
//				spendDataset.setClientCode(result.getColumnLatest("dataset".getBytes(), "CLIENTCODE".getBytes()).getValue().toString());
//				spendDataset.setPaymentType(result.getColumnLatest("dataset".getBytes(), "PAYMENTTYPE".getBytes()).getValue().toString());
//				spendDataset.setPaymentDate(result.getColumnLatest("dataset".getBytes(), "PAYMENTDATE".getBytes()).getValue().toString());
				spendDataset.setClientCode(result.getValue("dataset".getBytes(), "CLIENTCODE".getBytes()).toString());			
				return spendDataset;
			}
	    	
		});
	    JavaSQLContext sqlCtx = new JavaSQLContext(sparkContext);
	 // Apply a schema to an RDD of Java Beans and register it as a table.
	    JavaSchemaRDD schemaPeople = sqlCtx.applySchema(sd, SpendDataset.class);
	    
	    System.out.println("****************************************************");
	    System.out.println("HBASE QUERY");
	    RDDUtility.runSqlOnSchemaRDD(sqlCtx, schemaPeople);
	    
	    
	    
	    
	    
	    
	    /*SparkContext context = new SparkContext(sparkConf);
	    NewHadoopRDD<ImmutableBytesWritable, Result> hadoopRDD = new NewHadoopRDD<ImmutableBytesWritable, Result>(context, TableInputFormat.class, 
	    		ImmutableBytesWritable.class, Result.class, hbaseConf);
	    
	    JavaRDD<Tuple2<ImmutableBytesWritable, Result>>  javaHadoopRDD = hadoopRDD.toJavaRDD();
	    
	    JavaRDD<SpendDataset> sd = javaHadoopRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, SpendDataset>() {

			public SpendDataset call(Tuple2<ImmutableBytesWritable, Result> tuple)
					throws Exception {
				System.out.println(tuple);
				Result  result = tuple._2();
				
				SpendDataset spendDataset = new SpendDataset();
				spendDataset.setClientCode(result.getColumnLatest("dataset".getBytes(), "CLIENTCODE".getBytes()).getValue().toString());
				spendDataset.setPaymentType(result.getColumnLatest("dataset".getBytes(), "PAYMENTTYPE".getBytes()).getValue().toString());
				spendDataset.setPaymentDate(result.getColumnLatest("dataset".getBytes(), "PAYMENTDATE".getBytes()).getValue().toString());
								
				return spendDataset;
			}
		});
	    
	    JavaSQLContext sqlCtx = new JavaSQLContext(sparkContext);
		// Apply a schema to an RDD of Java Beans and register it as a table.
	    JavaSchemaRDD schemaPeople = sqlCtx.applySchema(sd, SpendDataset.class);
	    
	    System.out.println("****************************************************");
	    System.out.println("HBASE QUERY");
	    RDDUtility.runSqlOnSchemaRDD(sqlCtx, schemaPeople);*/
	}
}
