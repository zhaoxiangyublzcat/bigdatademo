import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 同现矩阵与得分矩阵相乘
 */
public class Step3 {
    public static boolean run(Configuration conf, Map<String, String> paths) {
        boolean ifExit = false;
        try {
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJobName("Step3");
            job.setJarByClass(Step3.class);

            job.setMapperClass(Step3Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(Step3Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path[]{new Path(paths.get("Step3Input1")), new Path(paths.get("Step3Input2"))});

            Path outPath = new Path(paths.get("Step3Output"));
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

    public static class Step3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        // 判断是同现矩阵还是用户得分矩阵
        private String flag;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            // 获取到数据名称
            flag = split.getPath().getParent().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = StringUtils.split(value.toString(), '\t');
            // 同现矩阵
            // 样本           商品1：商品2 n
            if (flag.equals("step2")) {
                String[] items = lines[0].split(":");
                String itemA = items[0];
                String itemB = items[1];
                String num = lines[1];
                // 商品1  A:商品2,n
                context.write(new Text(itemA), new Text("A:" + itemB + "," + num));
            } else if (flag.equals("step1")) {
                // 用户得分矩阵
                // 样本           用户  商品1:3,商品2:9,商品n:3
                String userId = lines[0];
                for (int i = 0; i < lines.length; i++) {
                    String[] itemAndRv = lines[i].split(":");
                    String item = itemAndRv[0];
                    String rv = itemAndRv[1];
                    // 商品1  B:用户名,3
                    context.write(new Text(item), new Text("B:" + userId + "," + rv));
                }
            }
        }
    }

    public static class Step3Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 同现矩阵
            Map<String, Integer> itemMatrix = new HashMap<>();
            // 用户矩阵
            Map<String, Integer> userMatrix = new HashMap<>();

            // 初始化两个矩阵
            // 样本1          商品1  A:商品2,n
            // 样本2          商品1  B:用户名,3
            for (Text t : values) {
                String line = t.toString();
                String[] kv = line.split(",");
                if (line.startsWith("A")) {
                    itemMatrix.put(kv[0], Integer.parseInt(kv[1]));
                } else if (line.startsWith("B")) {
                    userMatrix.put(kv[0], Integer.parseInt(kv[1]));
                }
            }

            double result = 0;
            Iterator<String> iterator = itemMatrix.keySet().iterator();
            while (iterator.hasNext()) {
                // item
                String item = iterator.next();
                // 同现的次数
                int num = itemMatrix.get(item).intValue();
                Iterator<String> it = userMatrix.keySet().iterator();
                while (it.hasNext()) {
                    String user = it.next();
                    int pre = userMatrix.get(user).intValue();
                    result = num + pre;
                    // 用户名  商品,对应得分
                    context.write(new Text(user), new Text(item + "," + result));
                }
            }
        }
    }
}
