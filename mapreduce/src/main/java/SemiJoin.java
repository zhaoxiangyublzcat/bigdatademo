import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SemiJoin extends Configured implements Tool {
    public static class SemiJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> joinKeys = new HashSet<String>();

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader bufferedReader = null;
            Path[] filePaths = context.getLocalCacheFiles();
            for (Path path : filePaths) {
                String pathStr = path.toString();
                bufferedReader = new BufferedReader(new FileReader(pathStr));
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] lineValues = StringUtils.split(line, "\t");
                    if (lineValues != null) {
                        joinKeys.add(lineValues[0]);
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String pathAddress = ((FileSplit) context.getInputSplit()).getPath().toString();

            if (pathAddress.endsWith("records-semi.txt")) {
                String[] lineValues = StringUtils.split(value.toString(), "\t");
                if (lineValues.length != 3) {
                    return;
                }
                keyOut.set(lineValues[0]);
                valueOut.set("records-semi.txt" + lineValues[1] + "\t" + lineValues[2]);
                context.write(keyOut, valueOut);
            }

            if (pathAddress.endsWith("station.txt")) {
                String[] lineValues = StringUtils.split(value.toString(), "\t");
                if (lineValues.length != 2) {
                    return;
                }
                keyOut.set(lineValues[0]);
                valueOut.set("station.txt" + lineValues[1]);
                context.write(keyOut, valueOut);
            }
        }
    }


    public static class SemiJoinRducer extends Reducer<Text, Text, Text, Text> {
        private List<String> leftList = new ArrayList<String>();
        private List<String> rightList = new ArrayList<String>();

        private Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            leftList.clear();
            rightList.clear();

            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.startsWith("records-semi.txt")) {
                    leftList.add(valueStr.replaceFirst("records-semi.txt", ""));
                }
                if (valueStr.startsWith("station.txt")) {
                    rightList.add(valueStr.replaceFirst("station.txt", ""));
                }
            }

            // 笛卡尔积
            for (String left : leftList) {
                for (String right : rightList) {
                    valueOut.set(left + "\t" + right);
                    context.write(key, valueOut);
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.out.println("输入的路径不符合要求");
            System.exit(1);
        }

        Path outPath = new Path(otherArgs[otherArgs.length - 1]);
        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已删除");
        }

        Job job = new Job(configuration, "SemiJoin");
        job.setJarByClass(SemiJoin.class);

        for (int i = 1; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outPath);

        job.addCacheFile(new URI(otherArgs[0]));

        job.setMapperClass(SemiJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SemiJoinRducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"hdfs://master:9000/input/secondSort/station.txt",
                "hdfs://master:9000/input/secondSort/records-semi.txt",
                "hdfs://master:9000/output/SemiJoinOutPut/"};
        int ifExit = ToolRunner.run(new SemiJoin(), paths);
        System.exit(ifExit);
    }
}
