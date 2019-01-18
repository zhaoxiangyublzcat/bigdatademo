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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author blz
 */
public class WordCountMain extends Configured implements Tool {

    public static class WordCountMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String line = value.toString();
            String[] lineArray = line.split(" ");
            for (String word : lineArray) {
                context.write(new Text(word), new Text("1"));
            }
        }
    }

    public static class WordCountReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            int sum = 0;
            for (Text num : values
            ) {
                sum += Integer.parseInt(num.toString());
            }
            context.write(new Text(key), new Text(String.valueOf(sum)));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, strings).getRemainingArgs();

        Job job = new Job(configuration, "wordCount");
        job.setJarByClass(WordCountMain.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outPath = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
            System.out.println("存在的路径已删除");
        }

        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] path = {args[0], args[1]};
        int ifExit = ToolRunner.run(new WordCountMain(), path);
        System.exit(ifExit);
    }
}
