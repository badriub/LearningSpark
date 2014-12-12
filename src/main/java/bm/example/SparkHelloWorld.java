package bm.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkHelloWorld {
	private static final String SPARK_HOME = "/home/badari/DEV/spark-1.1.0/";
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {		
		SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
		sparkConf.setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		String pathReadMe = SPARK_HOME+"README.md";
		
		
		JavaRDD<String> readmeFile = sparkContext.textFile(pathReadMe, 0);
		
		JavaRDD<String> words = readmeFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		List<Tuple2<String, Integer>> wordCounts = counts.collect();
		
		System.out.println("**********************************************");
		
		for(Tuple2<String, Integer> wordCount : wordCounts) {
			System.out.println(wordCount._1() + ": " + wordCount._2());
		}
		
		System.out.println("**********************************************");
		
		System.out.println("Number of lines in read me ="+readmeFile.count());
		
		JavaRDD<String> logData = sparkContext.textFile(pathReadMe).cache();

	    long numAs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
		public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	    	private static final long serialVersionUID = 1L;
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}

}
