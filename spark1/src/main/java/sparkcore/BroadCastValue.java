package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * 共享变量
 */
public class BroadCastValue {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadCastValue").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int f = 3;
        final Broadcast<Integer> fbc = sc.broadcast(f);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> resultRDD = listRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * fbc.getValue();
            }
        });

        resultRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }
}
