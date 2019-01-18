package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class GroupTopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GroupTopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lists = sc.textFile("hdfs://master:9000/in/grouptop.txt");
        JavaPairRDD<String, Integer> pairRDD = lists.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lines = s.split(" ");
                String groupName = lines[0];
                String number = lines[1];
                return new Tuple2<>(groupName, Integer.parseInt(number));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupPairRDD = pairRDD.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> groupTopNRDD = groupPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                List<Integer> list = new ArrayList<>();
                Iterable<Integer> nums = stringIterableTuple2._2;
                Iterator<Integer> it = nums.iterator();
                while (it.hasNext()) {
                    list.add(it.next());
                }

                Collections.sort(list, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return -(o1 - o2);
                    }
                });

                list = list.subList(0, 2);

                return new Tuple2<>(stringIterableTuple2._1, (Iterable<Integer>) list);
            }
        });

        groupTopNRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2);
            }
        });
    }
}
