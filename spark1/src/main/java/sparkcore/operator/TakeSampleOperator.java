package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TakeSampleOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TakeSampleOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6);

        JavaRDD<Integer> listRDD = sc.parallelize(list);
        List<Integer> takeSampleRDD = listRDD.takeSample(false, 3);

        for (Integer integer : takeSampleRDD) {
            System.out.println(integer);
        }
    }
}
