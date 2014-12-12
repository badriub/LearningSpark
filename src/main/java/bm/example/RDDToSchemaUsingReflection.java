package bm.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import bm.model.SpendDataset;
import bm.util.RDDUtility;

/**
 * Inferring the Schema Using Reflection
 * This class is an example of getting schema from RDD using declared class and reflection.
 * @author badari
 *
 */
public class RDDToSchemaUsingReflection {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
		sparkConf.setMaster("local");
	    
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> hdfsFile = sparkContext.textFile("hdfs://localhost:9000/sqoopimport");
		
		JavaRDD<SpendDataset> spendDatasets = mappingToSchema(hdfsFile);
		JavaSQLContext sqlCtx = new JavaSQLContext(sparkContext);
		// Apply a schema to an RDD of Java Beans and register it as a table.
	    JavaSchemaRDD schemaPeople = sqlCtx.applySchema(spendDatasets, SpendDataset.class);
	    
	    System.out.println("****************************************************");
	    System.out.println("Normal query on schema RDD");
	    RDDUtility.runSqlOnSchemaRDD(sqlCtx, schemaPeople);
	    // save in compressed format for efficiency
	    schemaPeople.saveAsParquetFile("spendDatasets.parquet1");
	   
	    JavaSchemaRDD parquetFileRDD = sqlCtx.parquetFile("spendDatasets.parquet1");
	    System.out.println("Using parquet file schema RDD");
	    RDDUtility.runSqlOnSchemaRDD(sqlCtx, parquetFileRDD);
	    System.out.println("****************************************************");
	    
	    /*hadoopRDD.map(new Function1<Tuple2<ImmutableBytesWritable,Result>, SpendDataset>() {
	    	public SpendDataset call(String line) throws Exception {
				String[] parts = line.split(",");
				SpendDataset spendDataset = new SpendDataset();
				spendDataset.setClientCode(parts[0]);
				spendDataset.setPaymentType(parts[1]);
				spendDataset.setPaymentDate(parts[2]);

		        return spendDataset;
			}

			public <A> Function1<Tuple2<ImmutableBytesWritable, Result>, A> andThen(
					Function1<SpendDataset, A> arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			
	    }, SpendDataset.class);*/
	    
	}

	private static JavaRDD<SpendDataset> mappingToSchema(
			JavaRDD<String> dataRDD) {
		// this code actually doesnt load any data into memory.
		JavaRDD<SpendDataset> spendDatasets = dataRDD.map(new Function<String, SpendDataset>()  {
			private static final long serialVersionUID = 1L;

			public SpendDataset call(String line) throws Exception {
				String[] parts = line.split(",");
				SpendDataset spendDataset = new SpendDataset();
				spendDataset.setClientCode(parts[0]);
				spendDataset.setPaymentType(parts[1]);
				spendDataset.setPaymentDate(parts[2]);

		        return spendDataset;
			}
		});
		return spendDatasets;
	}
}
