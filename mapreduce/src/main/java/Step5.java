import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * 排序找出前10
 */
public class Step5 {
    public static boolean run(Configuration conf, Map<String, String> paths) {
        boolean ifExit = false;
        try {
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJobName("Step5");
            job.setJarByClass(Step5.class);

            job.setMapperClass(Step5Mapper.class);
            job.setMapOutputKeyClass(PairWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setSortComparatorClass(Step5Sort.class);
            job.setGroupingComparatorClass(Step5Group.class);

            job.setReducerClass(Step5Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step5Input")));

            Path outPath = new Path(paths.get("Step5Output"));
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

    public static class PairWritable implements WritableComparable<PairWritable> {
        private String user;
        private Double num;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public Double getNum() {
            return num;
        }

        public void setNum(Double num) {
            this.num = num;
        }

        @Override
        public int compareTo(PairWritable o) {
            int userCompare = this.user.compareTo(o.getUser());
            if (userCompare == 0) {
                return -Double.compare(this.num, o.getNum());
            }
            return userCompare;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(user);
            dataOutput.writeDouble(num);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.user = dataInput.readUTF();
            this.num = dataInput.readDouble();
        }
    }

    public static class Step5Mapper extends Mapper<LongWritable, Text, PairWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = StringUtils.split(value.toString(), '\t');
            String user = lines[0];
            String item = lines[1];
            String num = lines[2];
            PairWritable keyOut = new PairWritable();
            keyOut.setUser(user);
            keyOut.setNum(Double.valueOf(num));
            context.write(keyOut, new Text(item + ":" + num));
        }
    }

    public static class Step5Sort extends WritableComparator {
        public Step5Sort() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable p1 = (PairWritable) a;
            PairWritable p2 = (PairWritable) b;

            int userCompare = p1.getUser().compareTo(p2.getUser());
            if (userCompare == 0) {
                return -Double.compare(p1.getNum(), p2.getNum());
            }
            return userCompare;
        }
    }

    public static class Step5Group extends WritableComparator {
        public Step5Group() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable p1 = (PairWritable) a;
            PairWritable p2 = (PairWritable) b;

            int userCompare = p1.getUser().compareTo(p2.getUser());

            return userCompare;
        }
    }

    public static class Step5Reducer extends Reducer<PairWritable, Text, Text, Text> {
        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            StringBuffer buffer = new StringBuffer();
            for (Text t : values) {
                if (i == 10) {
                    break;
                }
                buffer.append(t.toString() + ",");
                i++;
            }
            context.write(new Text(key.getUser()), new Text(buffer.toString()));
        }
    }
}
