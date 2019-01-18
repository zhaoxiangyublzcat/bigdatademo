package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class SaveAsTextFileOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SaveAsTextFileOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4);

        JavaRDD<Integer> parallelize = sc.parallelize(list);
        JavaRDD<Integer> map = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        map.saveAsTextFile("hdfs://hadoop:9000/sparkTestSaveFile");
    }
}
