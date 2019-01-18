import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondSort extends Configured implements Tool {
    /**
     * 自定义KeyOut类型
     */
    public static class IntPair implements WritableComparable<IntPair> {
        int first, second;

        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        public int compareTo(IntPair o) {
            if (first != o.getFirst()) {
                return first < o.first ? -1 : 1;
            } else if (second != o.getSecond()) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }

        // 序列化
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(first);
            dataOutput.writeInt(second);
        }

        // 反序列化
        public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readInt();
            second = dataInput.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IntPair)) return false;
            IntPair intPair = (IntPair) o;
            return first == intPair.first &&
                    second == intPair.second;
        }

        @Override
        public int hashCode() {
            return first * 157 + second;
        }
    }

    /**
     * 自定义Partitioner,根据InPair中的First实现分区
     */
    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {
        public int getPartition(IntPair intPair, IntWritable intWritable, int i) {
            return Math.abs(intPair.getFirst() * 127) % i;
        }
    }

//    自定义key二次排序类，可用自定义key类型中的compareTo代替
//    public static class KeyComarator extends WritableComparator

    /**
     * 自定义GroupingComparator类，实现分区内的数据分组
     */
    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair intPair_1 = (IntPair) a;
            IntPair intPair_2 = (IntPair) b;

            int left = intPair_1.getFirst();
            int right = intPair_2.getFirst();
            return left == right ? 0 : (left < right ? -1 : 1);
        }
    }

    /**
     * SecondSortMapper
     */
    public static class SecondSortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        private IntPair keyOut = new IntPair();
        private IntWritable valueOut = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] lines = value.toString().split(" ");
            int left = Integer.parseInt(lines[0]);
            int right = Integer.parseInt(lines[1]);

            keyOut.set(left, right);
            valueOut.set(right);
            context.write(keyOut, valueOut);
        }
    }

    /**
     * SecondSortReducer
     */
    public static class SecondSortReducer extends Reducer<IntPair, IntWritable, Text, IntWritable> {
        private Text keyOut = new Text();

        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws
                IOException, InterruptedException {
            keyOut.set(Integer.toString(key.getFirst()));

            for (IntWritable value : values) {
                context.write(keyOut, value);
            }
        }
    }


    public int run(String[] strings) throws Exception {
        Path inPath = new Path(strings[0]);
        Path outPath = new Path(strings[1]);

        Configuration configuration = new Configuration();
        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已删除");
        }

        Job job = new Job(configuration, "SecondSort");
        job.setJarByClass(SecondSort.class);

        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(SecondSortMapper.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SecondSortReducer.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {args[0], args[1]};
        int ifExit = ToolRunner.run(new SecondSort(), paths);
        System.exit(ifExit);
    }
}
