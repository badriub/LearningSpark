package bm.util;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class RDDUtility {

	public static void runSqlOnSchemaRDD(JavaSQLContext sqlCtx,
			JavaSchemaRDD schemaPeople) {
		long l = System.currentTimeMillis();
		schemaPeople.registerAsTable("SPENDANALYTICS_ABC");
	    
	    JavaSchemaRDD data = sqlCtx.sql("SELECT clientCode, paymentType, paymentDate FROM SPENDANALYTICS_ABC WHERE paymentType='CONTRACT'");
	    
	    
	    List<String[]> rawData = data.map(new Function<Row, String[]>() {
			private static final long serialVersionUID = 1L;

			public String[] call(Row row) throws Exception {
				String[] array = {row.getString(0), row.getString(1), row.getString(2)};
				return array;
			}
		}).collect();
	    
	    System.out.println("Count="+data.count());
	    System.out.println("Number of seconds taken:"+(System.currentTimeMillis() -l)/1000);
	}

	public static JavaSparkContext createSparkContext() {
		SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
		sparkConf.setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		return sparkContext;
	}
}
