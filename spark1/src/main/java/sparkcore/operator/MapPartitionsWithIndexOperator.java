package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

public class MapPartitionsWithIndexOperator {
    public static void main(String[] args) {
        // ****driver
        SparkConf conf = new SparkConf().setAppName("MapPartitionsWithIndexOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 测试数据
        List<String> names = Arrays.asList("zs", "ls", "ww");
        final Map<String, Integer> sorceMap = new HashMap<>();
        sorceMap.put("zs", 100);
        sorceMap.put("ls", 150);
        sorceMap.put("ww", 50);

        JavaRDD<String> nameRDD = sc.parallelize(names, 2);
        // driver****

        JavaRDD<String> nameWithPartitonIndexRDD = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String name = stringIterator.next();
                    String result = integer + ":" + name;
                    list.add(result);
                }
                return list.iterator();
            }
        }, true);

        JavaRDD<String> nameAndSourceWithPartitionIndexRDD = nameWithPartitonIndexRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            List<String> list = new ArrayList<>();

            @Override
            public Iterable<String> call(Iterator<String> stringIterator) throws Exception {
                while (stringIterator.hasNext()) {
                    String nameIndex = stringIterator.next();
                    String name = nameIndex.substring(nameIndex.indexOf(":")+1);
                    int source = sorceMap.get(name);
                    list.add(nameIndex + "-" + String.valueOf(source));
                }
                return (Iterable<String>) list.iterator();
            }
        });

        nameAndSourceWithPartitionIndexRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
