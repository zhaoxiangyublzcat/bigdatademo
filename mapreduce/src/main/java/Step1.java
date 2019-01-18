import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.Map;

/**
 * 获得每个用户的得分矩阵
 */
public class Step1 {
    public static boolean run(Configuration conf, Map<String, String> paths) {
        boolean ifExit = false;
        try {
            Job job = new Job(conf,"Step1");
            job.setJarByClass(Step1.class);

            job.setMapperClass(Step1Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(Step1Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));

            FileSystem fs = FileSystem.get(conf);
            Path outPath = new Path(paths.get("Step1Output"));
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

    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = StringUtils.split(value.toString(), '\t');
            String item = lines[0];
            String user = lines[1];
            String action = lines[2];

            Integer rv = StartRun.R.get(action);

            Text keyOut = new Text(user);
            Text valueOut = new Text(item + ":" + rv.intValue());
            context.write(keyOut, valueOut);
        }
    }

    public static class Step1Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> r = new HashMap<>();

            for (Text t : values) {
                String[] itemAndRv = t.toString().split(":");
                String item = itemAndRv[0];
                Integer action = Integer.parseInt(itemAndRv[1]);
                // 相同商品的得分累加（点击，加入购物车，购买）
                action = ((Integer) (r.get(item) == null ? 0 : r.get(item))).intValue() + action;
                r.put(item, action);
            }

            StringBuffer sb = new StringBuffer();
            for (Map.Entry<String, Integer> entry : r.entrySet()) {
                sb.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");
                // 商品id:得分
            }
            context.write(key, new Text(sb.toString()));
        }
    }
}
