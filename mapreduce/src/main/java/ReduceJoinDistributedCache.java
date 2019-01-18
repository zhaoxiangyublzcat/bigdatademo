import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

/**
 * 通过分布式缓存实现ReduceJoin
 * 适用场景：其中一个表可以放到内存中
 * 此处station.txt作为小表缓存
 * records.txt作为大表
 */
public class ReduceJoinDistributedCache extends Configured implements Tool {
    public static class ReduceJoinDistributedCacheMapper extends Mapper<LongWritable, Text, Text,
            Text> {
        private Text keyOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] valueLines = StringUtils.split(value.toString(), "\t");

            if (valueLines.length != 3) {
                System.out.println("行值个数不符合要求");
                System.exit(1);
            }
            keyOut.set(valueLines[0]);
            context.write(keyOut, value);
        }
    }

    public static class ReduceJoinDistributedCacheReducer extends Reducer<Text, Text, Text, Text> {
        private Hashtable<String, String> table = new Hashtable<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader reader = null;
            String line = null;
            // 返回缓存文件路径
            Path[] cacheFilePaths = context.getLocalCacheFiles();
            for (Path path : cacheFilePaths) {
                String pathStr = path.toString();
                System.out.println(pathStr);
                // FIXME:缓存文件运行后不存在（仍含有路径但是所需文件不存在）
                reader = new BufferedReader(new FileReader(pathStr));
                while ((line = reader.readLine()) != null) {
                    String[] stationLines = StringUtils.split(line, "\\s+");
                    if (stationLines != null) {
                        table.put(stationLines[0], stationLines[1]);
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            String stationName = table.get(key.toString());
            for (Text value : values) {
                context.write(new Text(stationName), value);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.out.println("输入的路径太少，程序终止");
            System.exit(2);
        }

        Path outPath = new Path(otherArgs[otherArgs.length - 1]);
        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已删除");
        }

        Job job = new Job(configuration, "ReduceJoinDistributedCache");
        job.setJarByClass(ReduceJoinDistributedCache.class);

        // 设置job缓存文件
        job.addCacheFile(new Path(otherArgs[0]).toUri());

        job.setMapperClass(ReduceJoinDistributedCacheMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReduceJoinDistributedCacheReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 1; i < otherArgs.length - 1; i++) {
            FileInputFormat.setInputPaths(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/MRJoin/station.txt",
                "/Users/hadoop/Documents/HadoopIO/in/MRJoin/records.txt",
                "/Users/hadoop/Documents/HadoopIO/out/ReduceJoinDistributedCacheOutPut/"};
        int ifExit = ToolRunner.run(new ReduceJoinDistributedCache(), paths);
        System.exit(ifExit);
    }
}
