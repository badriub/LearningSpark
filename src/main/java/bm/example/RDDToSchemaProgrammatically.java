package bm.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.types.StructField;
//import org.apache.spark.sql.api.java.DataType;

public class RDDToSchemaProgrammatically {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("RDDToSchemaProgrammatically");
		sparkConf.setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> hdfsFile = sparkContext.textFile("hdfs://localhost:9000/sqoopimport");
		
		// create the schema in a string
		String schemaString = "clientCode paymentType paymentDate";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
		  //fields.add(DataType.createStructField(fieldName, DataType	.StringType, true));
		}
		
		// TODO: once you get spark core 1.1 complete this approach
	}

}
