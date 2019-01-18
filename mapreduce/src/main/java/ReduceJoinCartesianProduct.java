import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 流程：两个大表，通过笛卡尔积实现reduce join
 * 适用场景：两个表的链接字段Key都不唯一（包含一对多，多对多）
 */
public class ReduceJoinCartesianProduct extends Configured implements Tool {
    /**
     * 为来自
     */
    public static class ReduceJoinCartesianProductMapper extends Mapper<Object, Text, Text, Text> {
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String path = ((FileSplit) context.getInputSplit()).getPath().toString();
            if (path.endsWith("records.txt")) {
                String[] lineValues = StringUtils.split(value.toString(), '\t');
                keyOut.set(lineValues[0]);
                valueOut.set("records.txt" + "\t" + lineValues[1] + "\t" + lineValues[2]);
                context.write(keyOut, valueOut);
            }
            if (path.endsWith("station.txt")) {
                String[] lineValues = StringUtils.split(value.toString(), '\t');
                keyOut.set(lineValues[0]);
                valueOut.set("station.txt" + "\t" + lineValues[1]);
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class ReduceJoinCartesianProductReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> leftList = new ArrayList<String>();
        private List<String> rightList = new ArrayList<String>();
        private Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            leftList.clear();
            rightList.clear();

            for (Text value : values) {
                if (value.toString().startsWith("station.txt")) {
                    leftList.add(value.toString().replaceFirst("station.txt", ""));
                }
                if (value.toString().startsWith("records.txt")) {
                    rightList.add(value.toString().replaceFirst("records.txt", ""));
                }
            }

            // 笛卡尔积
            for (String string : leftList) {
                for (String string2 : rightList) {
                    valueOut.set(string + "\t" + string2);
                    context.write(key, valueOut);
                }
            }
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();

        // 获得传入的路径
        String[] otherArgs = new GenericOptionsParser(configuration, strings).getRemainingArgs();
        if (otherArgs.length <= 2) {
            System.out.println("输入的文件路径太少，无法实现合并");
            // 非正常退出
            System.exit(2);
        }

        // 输出路径如果存在则删除
        Path outPath = new Path(otherArgs[otherArgs.length - 1]);
        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径删除成功");
        }

        Job job = new Job(configuration, "ReduceJoinCartesianProduct");
        job.setJarByClass(ReduceJoinCartesianProduct.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        job.setMapperClass(ReduceJoinCartesianProductMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReduceJoinCartesianProductReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/MRJoin/station.txt",
                "/Users/hadoop/Documents/HadoopIO/in/MRJoin/records.txt",
                "/Users/hadoop/Documents/HadoopIO/out/ReduceJoinCartesianProductOut/"};
        int ifExit = ToolRunner.run(new ReduceJoinCartesianProduct(), paths);
        System.exit(ifExit);
    }
}
