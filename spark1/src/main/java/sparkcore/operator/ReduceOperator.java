package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ReduceOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ReduceOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numlist = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numRDD = sc.parallelize(numlist);

        int sum = numRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println(sum);
        sc.close();
    }
}
