package sparkcore.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 合并算子
 */
public class CoalesceOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CoalesceOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> peoples = Arrays.asList("1p", "2p", "3p", "4p", "5p", "6p", "7p", "8p", "9p", "10p", "11p", "12p");

        JavaRDD<String> peoplesRDD = sc.parallelize(peoples, 6);
        //展示每个人所属的部门
        JavaRDD<String> departmentPeopleRDD = peoplesRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String people = stringIterator.next();
                    list.add("部门[" + (integer + 1) + "]---" + people);
                }
                return list.iterator();
            }
        }, true);

        // 将partitions的数量减少
        JavaRDD<String> departmentPeopleCoalesceRDD = departmentPeopleRDD.coalesce(3);
        JavaRDD<String> departmentPeopleCoalesceRDD2 = departmentPeopleCoalesceRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String people = stringIterator.next();
                    list.add("部门[" + (integer + 1) + "]---原" + people);
                }
                return list.iterator();
            }
        }, true);

        departmentPeopleCoalesceRDD2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
