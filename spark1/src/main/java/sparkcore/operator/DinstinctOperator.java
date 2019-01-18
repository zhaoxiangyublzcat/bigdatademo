package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 去重
 */
public class DinstinctOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DinstinctOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("zs","ls","zs","ls","ww");
        JavaRDD<String> listRDD = sc.parallelize(list);
        listRDD.distinct().foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
