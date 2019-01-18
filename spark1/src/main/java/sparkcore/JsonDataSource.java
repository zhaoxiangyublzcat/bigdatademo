package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

/**
 * FIXME：因Spark升级到2.0出错
 */
public class JsonDataSource {
//    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("JsonDataSource").setMaster("master");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        SQLContext sqlContext = new SQLContext(sc);
//
//        DataFrame sourceDF = sqlContext.read().json(args[0]);
//        // 注册临时表
//        sourceDF.registerTempTable("student_sources");
//        DataFrame stusNameDF = sqlContext.sql("select name, source from student_sources where source >=80");
//        // 抓换成String
//        List<String> studsNameList = stusNameDF.toJavaRDD().map(new Function<Row, String>() {
//            @Override
//            public String call(Row row) throws Exception {
//                return row.getString(0);
//            }
//        }).collect();
//
//        List<String> studsInfoJSON = new ArrayList<>();
//        studsInfoJSON.add("[{\"name\":\"张三\",\"age\":19},");
//        studsInfoJSON.add("{\"name\":\"李四\",\"age\":5},");
//        studsInfoJSON.add("{\"name\":\"王五\",\"age\":10},");
//        studsInfoJSON.add("{\"name\":\"王五\",\"age\":10},");
//        studsInfoJSON.add("{\"name\":\"赵六\",\"age\":10},");
//        studsInfoJSON.add("{\"name\":\"周七\",\"age\":10},");
//        studsInfoJSON.add("{\"name\":\"念深\",\"age\":109}]");
//        JavaRDD<String> studsInfoRDD = sc.parallelize(studsInfoJSON);
//        DataFrame studsInfoDF = sqlContext.read().json(studsInfoRDD);
//        studsInfoDF.registerTempTable("studsInfo_sources");
//        String sql = "select name,age from studsInfo_sources where name in(";
//        for (int i = 0; i < studsNameList.size(); i++) {
//            sql += "'" + studsNameList.get(i) + "'";
//            if (i < studsNameList.size() - 1) {
//                sql += ",";
//            }
//        }
//
//        sql += ")";
//        System.out.println(sql);
//
////        Dataset<Row> studsFilterInfoDF = sqlContext.sql(sql);
////        studsFilterInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
////            @Override
////            public Tuple2<String, Integer> call(Row row) throws Exception {
////                return new Tuple2<>(row.getString(0),row.getInt(1));
////            }
////        }).join();
//    }
}
