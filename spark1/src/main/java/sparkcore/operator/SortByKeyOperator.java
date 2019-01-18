package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SortByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SortByKeyOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer,String>> list = Arrays.asList(
                new Tuple2<Integer, String>(150,"zs"),
                new Tuple2<Integer, String>(350,"etet"),
                new Tuple2<Integer, String>(10,"dsfs"),
                new Tuple2<Integer, String>(100,"zsdf")
        );

        JavaPairRDD<Integer, String> integerStringJavaPairRDD = sc.parallelizePairs(list);

        // false 倒叙  true 正序
        integerStringJavaPairRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._1+"---"+integerStringTuple2._2);
            }
        });
    }
}
