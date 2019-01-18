import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 通过二次排序实现ReducerJoin
 * 适用场景：其中一个表的链接字段Key唯一
 */
public class ReducerJoinSecondSort extends Configured implements Tool {
    /**
     * 自定义TextPair类，作为keyOut输出
     */
    public static class TextPair implements WritableComparable<TextPair> {
        private Text first, second;

        public TextPair() {
            set(new Text(), new Text());
        }

        public TextPair(Text first, Text second) {
            set(new Text(first), new Text(second));
        }

        public void set(Text first, Text second) {
            this.first = first;
            this.second = second;
        }

        public Text getFirst() {
            return first;
        }

        public Text getSecond() {
            return second;
        }

        public int compareTo(TextPair o) {
            if (!first.equals(o.first)) {
                return first.compareTo(o.first);
            } else if (!second.equals(o.second)) {
                return second.compareTo(o.second);
            } else {
                return 0;
            }
        }

        // 序列化
        public void write(DataOutput dataOutput) throws IOException {
            first.write(dataOutput);
            second.write(dataOutput);
        }

        // 反序列化
        public void readFields(DataInput dataInput) throws IOException {
            first.readFields(dataInput);
            second.readFields(dataInput);
        }

        @Override
        public int hashCode() {
            return first.hashCode() * 163 + second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TextPair) {
                TextPair textPair = (TextPair) obj;
                return first.equals(textPair.first) && second.equals(textPair.second);
            }
            return false;
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }
    }

    public static class StationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
        private TextPair keyOut = new TextPair();
        private Text left = new Text();
        private Text right = new Text();
        private Text valueOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] lines = value.toString().split("\t");
            if (lines.length == 2) {
                left.set(lines[0]);
                right.set("0");
                keyOut.set(left, right);
                valueOut.set(lines[1]);
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class RecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
        private TextPair keyOut = new TextPair();
        private Text left = new Text();
        private Text right = new Text();
        private Text valueOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] lines = value.toString().split("\t");
            if (lines.length == 3) {
                left.set(lines[0]);
                right.set("1");
                keyOut.set(left, right);
                valueOut.set(lines[1] + "\t" + lines[2]);
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class KeyPartitioner extends Partitioner<TextPair, Text> {

        public int getPartition(TextPair textPair, Text text, int i) {
            return (textPair.getFirst().hashCode() & Integer.MAX_VALUE) % i;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TextPair textPair_1 = (TextPair) a;
            TextPair textPair_2 = (TextPair) b;
            Text left = textPair_1.getFirst();
            Text right = textPair_2.getFirst();

            return left.compareTo(right);
        }
    }

    public static class ReducerJoinSecondSortReducer extends Reducer<TextPair, Text, Text, Text> {
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void reduce(TextPair key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            String[] keyLine = key.toString().split("\t");
            keyOut.set(keyLine[0]);
            String line = "";
            for (Text value : values) {
                line += value.toString() + " ";
            }
            valueOut.set(line);
            context.write(keyOut, valueOut);
        }
    }


    public int run(String[] strings) throws Exception {
        Path stationPath = new Path(strings[0]);
        Path recordPath = new Path(strings[1]);
        Path outPath = new Path(strings[2]);

        Configuration configuration = new Configuration();
        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已经清除");
        }

        Job job = new Job(configuration, "ReducerJoinSecondSort");
        job.setJarByClass(ReducerJoinSecondSort.class);

        MultipleInputs.addInputPath(job, stationPath, TextInputFormat.class, StationMapper.class);
        MultipleInputs.addInputPath(job, recordPath, TextInputFormat.class, RecordMapper.class);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(KeyPartitioner.class);

        job.setReducerClass(ReducerJoinSecondSortReducer.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/MRJoin/station.txt",
                "/Users/hadoop/Documents/HadoopIO/in/MRJoin/records.txt",
                "/Users/hadoop/Documents/HadoopIO/out/ReducerJoinSecondSortOut"};
        int ifExit = ToolRunner.run(new ReducerJoinSecondSort(), paths);
        System.exit(ifExit);
    }
}
