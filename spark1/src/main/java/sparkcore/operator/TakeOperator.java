package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TakeOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TakeOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> listRDD = sc.parallelize(list);
        List<Integer> listTakeRDD = listRDD.take(3);

        for (Integer integer : listTakeRDD) {
            System.out.println(integer);
        }

    }
}
