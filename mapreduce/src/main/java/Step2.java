import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Map;

/**
 * 获取同现矩阵
 */
public class Step2 {
    private final static IntWritable valueOut = new IntWritable(1);

    public static boolean run(Configuration conf, Map<String, String> paths) {
        boolean ifExit = false;
        try {
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJobName("Step2");
            job.setJarByClass(Step2.class);

            job.setMapperClass(Step2Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setReducerClass(Step2Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));

            Path outPath = new Path(paths.get("Step2Output"));
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);
            ifExit = job.waitForCompletion(true);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
        return ifExit;
    }

    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 用户名  商品1:3,商品2:4,商品n:5
            String[] lines = StringUtils.split(value.toString(), '\t');
            // 商品 1
            String[] items = lines[1].split(",");
            for (int i = 0; i < items.length; i++) {
                // 商品
                String itemA = items[i].split(":")[0];
                for (int j = 0; j < items.length; j++) {
                    String itemB = items[j].split(":")[0];
                    context.write(new Text(itemA + ":" + itemB), valueOut);
                }
            }
        }
    }

    public static class Step2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            valueOut.set(sum);
            // 商品1：商品2  3
            context.write(key, valueOut);
        }
    }

}
