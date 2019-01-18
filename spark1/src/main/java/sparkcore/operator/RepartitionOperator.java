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
 * 重新分配算子
 */
public class RepartitionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CoalesceOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> peoples = Arrays.asList("1p", "2p", "3p", "4p", "5p", "6p", "7p", "8p", "9p", "10p", "11p", "12p");

        JavaRDD<String> peoplesRDD = sc.parallelize(peoples, 3);
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

        // 重新分配partitions
        JavaRDD<String> departmentPeopleRepartitionRDD = departmentPeopleRDD.repartition(6);

        JavaRDD<String> departmentPeopleRepartitionRDD2 = departmentPeopleRepartitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
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

        departmentPeopleRepartitionRDD2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
