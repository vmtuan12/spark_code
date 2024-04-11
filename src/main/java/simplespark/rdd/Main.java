package simplespark.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        String appName = "simplespark";
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("spark://mhtuan-HP:7077");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> textFile = sc.textFile("hdfs://localhost:9000/spark_wordcount/input/spark_wordcount_input.txt");
            JavaPairRDD<String, Integer> result = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a,b) -> (a + b));

            result.saveAsTextFile("hdfs://localhost:9000/spark_wordcount/output/result");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}