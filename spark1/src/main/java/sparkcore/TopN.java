package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class TopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://master:9000/in/top.txt");
        // kye:100  value:"100"
        JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<>(Integer.valueOf(s), s);
            }
        });
        // 倒叙后取得前三个数据的value值
        List<String> results = pairs.sortByKey(false).map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2._2;
            }
        }).take(3);
        for (String result : results) {
            System.out.println(result);
        }

        sc.close();
    }
}
