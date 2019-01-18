package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class SampleOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SampleOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("zs","ls","ww","zl","zw","sz","tt","xk");

        JavaRDD<String> listRDD = sc.parallelize(list);
        // false 不重复，  true 重复
        listRDD.sample(true,0.7).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }

}
