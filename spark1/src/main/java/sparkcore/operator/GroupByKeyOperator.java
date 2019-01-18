package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GroupByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GroupByKeyOperator").setMaster("local");
        conf.set("spark.default.parallelism","5");
        JavaSparkContext sc = new JavaSparkContext(conf);


        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("zs", 500),
                new Tuple2<String, Integer>("ls", 100),
                new Tuple2<String, Integer>("ww", 50),
                new Tuple2<String, Integer>("ww", 300)
        );

        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> mapRDD = listRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._1, stringIntegerTuple2._2 + 2);
            }
        });

        JavaPairRDD<String, Integer> addRDDMapRDD = mapRDD.repartition(10);

        JavaPairRDD<String, Integer> pairRDD = addRDDMapRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._1, stringIntegerTuple2._2 + 2);
            }
        });

        JavaPairRDD<String, Iterable<Integer>> resultRDD = pairRDD.groupByKey();

        resultRDD.collect();

        sc.close();
    }
}
