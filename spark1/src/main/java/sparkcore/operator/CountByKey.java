package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CountByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CountByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2<String, String>("70s", "张三"),
                new Tuple2<String, String>("70s", "李四"),
                new Tuple2<String, String>("70s", "王五"),
                new Tuple2<String, String>("70s", "赵六"),
                new Tuple2<String, String>("80s", "奇兵"),
                new Tuple2<String, String>("80s", "布阵")
        );

        JavaPairRDD<String, String> stringStringJavaPairRDD = sc.parallelizePairs(list);
        Map<String, Object> stringLongMap = stringStringJavaPairRDD.countByKey();
        for (Map.Entry<String, Object> entry : stringLongMap.entrySet()) {
            System.out.println(entry.getKey()+"---"+entry.getValue());
        }


    }
}
