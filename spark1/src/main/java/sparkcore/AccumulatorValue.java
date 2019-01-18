package sparkcore;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * 全局累加器
 */
public class AccumulatorValue {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccumulatorValue").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final Accumulator<Integer> sum = sc.accumulator(0, "0 accumulator");
        JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        listRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });
        System.out.println(sum);
    }
}
