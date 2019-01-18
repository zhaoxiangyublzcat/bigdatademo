import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

/**
 * 通过分布式缓存（distributedCache）实现Map Join
 * 适用场景：大表和小表
 */
public class MapJoinDistributedCache extends Configured implements Tool {

    public static class MapJoinDistributedCacheMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Hashtable<String, String> table = new Hashtable<String, String>();

        private Text keyOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] filePath = context.getLocalCacheFiles();
            if (filePath.length == 0) {
                throw new FileNotFoundException("未发现缓存文件");
            }

            FileSystem fileSystem = FileSystem.getLocal(context.getConfiguration());

            FSDataInputStream in = fileSystem.open(new Path(filePath[0].toString()));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] lineValues = StringUtils.split(line, "\t");
                table.put(lineValues[0], lineValues[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineValues = StringUtils.split(value.toString(), "\t");
            String stationName = table.get(lineValues[0]);
            if (stationName ==null){
                return;
            }
            keyOut.set(stationName);
            context.write(keyOut, value);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, strings).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.out.println("输入路径不符合要求");
            System.exit(1);
        }
        Path outPath = new Path(otherArgs[otherArgs.length - 1]);
        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已删除");
        }

        Job job = new Job(configuration, "MapJoinDistributedCache");
        job.setJarByClass(MapJoinDistributedCache.class);

        for (int i = 1; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outPath);

        job.addCacheFile(new URI(otherArgs[0]));

        job.setMapperClass(MapJoinDistributedCacheMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/MRJoin/station.txt",
                "/Users/hadoop/Documents/HadoopIO/in/MRJoin/records.txt",
                "/Users/hadoop/Documents/HadoopIO/out/MapJoinDistributedCacheOutPut/"};
        int ifExit = ToolRunner.run(new MapJoinDistributedCache(), paths);
        System.exit(ifExit);
    }
}
