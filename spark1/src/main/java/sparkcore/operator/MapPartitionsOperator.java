package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

public class MapPartitionsOperator {
    public static void main(String[] args) {
        // ****driver
        SparkConf conf = new SparkConf().setAppName("MapOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 测试数据
        List<String> names = Arrays.asList("zs", "ls", "ww");
        final Map<String, Integer> sorceMap = new HashMap<>();
        sorceMap.put("zs", 100);
        sorceMap.put("ls", 150);
        sorceMap.put("ww", 50);

        JavaRDD<String> nameRDD = sc.parallelize(names);
        // driver****
        final JavaRDD<Integer> sourceRDD = nameRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            List<Integer> list = new ArrayList<>();

            @Override
            public Iterable<Integer> call(Iterator<String> stringIterator) throws Exception {
                while (stringIterator.hasNext()) {
                    String name = stringIterator.next();
                    Integer source = sorceMap.get(name);
                    list.add(source);
                }
                return (Iterable<Integer>) list.iterator();
            }
        });

        sourceRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }
}
