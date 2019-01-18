package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * reducebykey = groupbykey+reduce
 */
public class ReduceByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ReduceByKeyOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        List<Tuple2<String, Integer>> sourceList = Arrays.asList(
                new Tuple2<String, Integer>("zs", 100),
                new Tuple2<String, Integer>("ls", 10),
                new Tuple2<String, Integer>("ww", 200),
                new Tuple2<String, Integer>("ww", 150)
        );

        JavaPairRDD<String, Integer> sourceRDD = sc.parallelizePairs(sourceList);
        sourceRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

    }
}
