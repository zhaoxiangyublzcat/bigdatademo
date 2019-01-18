package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 笛卡尔积
 */
public class CartesianOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CartesianOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> c1 = Arrays.asList("你好", "再见", "欢迎", "拜拜");
        List<String> c2 = Arrays.asList("张三", "李四", "王五", "赵六");

        JavaRDD<String> c1RDD = sc.parallelize(c1);
        JavaRDD<String> c2RDD = sc.parallelize(c2);

        c1RDD.cartesian(c2RDD).foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2._1 + "---" + stringStringTuple2._2);
            }
        });

    }
}
