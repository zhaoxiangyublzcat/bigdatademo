package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class FlatMapOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FlatMapOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> lines = Arrays.asList("hello lisi", "bye ww", "hello", "haha !!!");
        JavaRDD<String> linesRDD = sc.parallelize(lines);
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return (Iterable<String>) Arrays.asList(s.split(" ")).iterator();
            }
        });

        wordsRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
