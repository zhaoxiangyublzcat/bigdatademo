import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 明星热度计算
 */
public class Star extends Configured implements Tool {
    public static class StarMapper extends Mapper<Object, Text, Text, Text> {
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineValues = StringUtils.split(value.toString(), "\\s+");
            if (lineValues.length != 3) {
                return;
            }
            keyOut.set(lineValues[1]);
            valueOut.set(lineValues[0] + "\t" + lineValues[1]);
            context.write(keyOut, valueOut);
        }
    }

    public static class StarCombiner extends Reducer<Text, Text, Text, Text> {
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            int indexHot = 0;
            String name = null;
            for (Text value : values) {
                String[] nameAndIndexHot = StringUtils.split(value.toString(), "\\s+");
                name = nameAndIndexHot[0];
                indexHot += Integer.parseInt(nameAndIndexHot[1]);
            }
            valueOut.set(name + "\t" + indexHot);
            context.write(key, valueOut);
        }
    }

    public static class StarPartitioner extends Partitioner<Text, Text> {

        public int getPartition(Text text, Text text2, int numPartitions) {
            String sex = text.toString();
            if (numPartitions == 0) {
                return 0;
            }
            if (sex.equals("male")) {
                return 0;
            }
            if (sex.equals("female")) {
                return 1 % numPartitions;
            }
            return 2 % numPartitions;
        }
    }

    public static class StarReducer extends Reducer<Text, Text, Text, Text> {
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            int indexHot = 0;
            String name = null;
            for (Text value : values) {
                String[] nameAndIndexHot = StringUtils.split(value.toString(), "\\s+");
                keyOut.set(nameAndIndexHot[0]);
                indexHot += Integer.parseInt(nameAndIndexHot[1]);

            }
            valueOut.set(key + "\t" + indexHot);
            context.write(keyOut, valueOut);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Path outPath = new Path(args[1]);
        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已删除");
        }

        Job job = new Job(configuration, "Star");
        job.setJarByClass(Star.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(StarMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(StarPartitioner.class);

        job.setReducerClass(StarReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/actor.txt",
                "/Users/hadoop/Documents/HadoopIO/out/StarOutput/"};
        int ifExit = ToolRunner.run(new Star(), paths);
        System.exit(ifExit);
    }
}
