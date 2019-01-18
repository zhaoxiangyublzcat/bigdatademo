import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author blz
 */
public class WeatherMAX {
    public static class wMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String year = value.toString().substring(0, 4);
            int weather = Integer.parseInt(value.toString().substring(8));

            context.write(new Text(year), new IntWritable(weather));
        }
    }

    public static class wReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxW = 0;
            for (IntWritable value : values) {
                if (value.get() > maxW) {
                    maxW = value.get();
                }
            }
            context.write(key, new IntWritable(maxW));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration c = new Configuration();

        Job job = new Job(c, "w");
        job.setJarByClass(WeatherMAX.class);

        job.setMapperClass(wMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(wReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/Users/blz/test/d"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/blz/test/out"));

        job.waitForCompletion(true);
    }
}
