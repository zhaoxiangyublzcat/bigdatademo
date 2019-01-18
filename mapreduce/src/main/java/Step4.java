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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 同现矩阵与得分矩阵相乘之后相加
 */
public class Step4 {
    public static boolean run(Configuration conf, Map<String, String> paths) {
        boolean ifExit = false;
        try {
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJobName("Step4");
            job.setJarByClass(Step4.class);

            job.setMapperClass(Step4Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setReducerClass(Step4Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step4Input")));

            Path outPath = new Path(paths.get("Step4Output"));
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

    public static class Step4Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = StringUtils.split(value.toString(), '\t');
            context.write(new Text(lines[0]), new Text(lines[1]));
        }
    }

    public static class Step4Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap<>();

            for (Text t : values) {
                // 商品,相对应得分
                String[] lines = t.toString().split(",");
                String item = lines[0];
                Double source = Double.parseDouble(lines[1]);

                if (map.containsKey(item)) {
                    map.put(item, map.get(item) + source);
                } else {
                    map.put(item, source);
                }
            }

            Iterator<String> it = map.keySet().iterator();
            while (it.hasNext()) {
                String item = it.next();
                double source = map.get(item);
                context.write(key, new Text(item + "," + source));
            }
        }
    }
}
