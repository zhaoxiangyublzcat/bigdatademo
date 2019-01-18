import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MapReduceOutputFormat extends Configured implements Tool {
    public static class MapReduceOutputFormatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, one);
        }
    }

    public static class MapReduceOutputFormatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs multipleOutputs = null;
        private IntWritable valueOut = new IntWritable();

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 120@qq.com
            String mail_address_str = key.toString();
            int begin = mail_address_str.indexOf("@");
            int end = mail_address_str.indexOf(".");
            if (begin > end) {
                return;
            }
            // 邮箱个数总和
            int sum = 0;
            // 邮箱名称
            String mailName = mail_address_str.substring(begin + 1, end);
            for (IntWritable value : values) {
                sum += value.get();
            }
            valueOut.set(sum);
            multipleOutputs.write(key, valueOut, mailName);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Path outPath = new Path(otherArgs[otherArgs.length - 1]);

        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已删除");
        }

        Job job = new Job(configuration, "MapReduceOutputFormat");
        job.setJarByClass(MapReduceOutputFormat.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(MapReduceOutputFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MapReduceOutputFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/email.txt", "/Users/hadoop/Documents/HadoopIO/out/MapReduceOutputFormatOutput/"};
        int ifExit = ToolRunner.run(new MapReduceOutputFormat(), paths);
        System.exit(ifExit);
    }
}
