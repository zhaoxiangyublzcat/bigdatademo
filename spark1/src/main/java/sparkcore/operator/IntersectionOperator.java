package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 取得交集
 */
public class IntersectionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("IntersectionOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list1 = Arrays.asList("1","2","3","4","5");
        List<String> list2 = Arrays.asList("2","4");

        JavaRDD<String> parallelize1 = sc.parallelize(list1);
        JavaRDD<String> parallelize2 = sc.parallelize(list2);

        parallelize1.intersection(parallelize2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
