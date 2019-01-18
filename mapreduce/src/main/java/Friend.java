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

public class Friend {
    /**
     * 过滤二度关系重复数据
     */
    public static class FriendFormat {
        public static String format(String f1, String f2) {
            int friendCompare = f1.compareTo(f2);
            if (friendCompare > 0) {
                return f1 + "-" + f2;
            } else {
                return f2 + "-" + f1;
            }
        }
    }

    /**
     * 自定义mapper类
     */
    public static class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = StringUtils.split(value.toString(), ' ');

            for (int i = 0; i < lines.length; i++) {
                // 已知关系
                String s1 = FriendFormat.format(lines[0], lines[i]);
                context.write(new Text(s1), new IntWritable(0));
                for (int j = i + 1; j < lines.length; j++) {
                    // 二度关系
                    String s2 = FriendFormat.format(lines[i], lines[j]);
                    context.write(new Text(s2), new IntWritable(1));
                }
            }
        }
    }

    /**
     * 自定义reducer类
     */
    public static class FriendReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            boolean flag = true;
            for (IntWritable iw : values) {
                if (iw.get() == 0) {
                    flag = false;
                    break;
                } else {
                    sum += iw.get();
                }
            }

            if (flag) {
                String outKey = key.toString() + "-" + sum;
                context.write(new Text(outKey), NullWritable.get());
            }
        }
    }

    public static boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "friend");
        job.setJarByClass(Friend.class);

        job.setMapperClass(FriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(FriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/in/friend"));
        Path outPath = new Path("/out/FriendOut/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
            System.out.println("存在的路径已删除");
        }

        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true);
    }

    public static class FriendBean implements WritableComparable<FriendBean> {
        private String friend1;
        private String friend2;
        private int friendHot;

        public String getFriend1() {
            return friend1;
        }

        public void setFriend1(String friend1) {
            this.friend1 = friend1;
        }

        public String getFriend2() {
            return friend2;
        }

        public void setFriend2(String friend2) {
            this.friend2 = friend2;
        }

        public int getFriendHot() {
            return friendHot;
        }

        public void setFriendHot(int friendHot) {
            this.friendHot = friendHot;
        }

        @Override
        public int compareTo(FriendBean o) {
            int f1Compare = this.friend1.compareTo(o.getFriend1());
            if (f1Compare == 0) {
                return Integer.compare(this.friendHot, o.getFriendHot());
            }

            return f1Compare;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(friend1);
            dataOutput.writeUTF(friend2);
            dataOutput.writeInt(friendHot);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.friend1 = dataInput.readUTF();
            this.friend2 = dataInput.readUTF();
            this.friendHot = dataInput.readInt();
        }
    }


    public static class FriendMapper2 extends Mapper<LongWritable, Text, FriendBean, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = StringUtils.split(value.toString(), '-');
            FriendBean friendBean = new FriendBean();
            friendBean.setFriend1(lines[0]);
            friendBean.setFriend2(lines[1]);
            friendBean.setFriendHot(Integer.parseInt(lines[2]));
            context.write(friendBean, new IntWritable(Integer.parseInt(lines[2])));

            FriendBean friendBean2 = new FriendBean();
            friendBean.setFriend1(lines[1]);
            friendBean.setFriend2(lines[0]);
            friendBean.setFriendHot(Integer.parseInt(lines[2]));
            context.write(friendBean2, new IntWritable(Integer.parseInt(lines[2])));
        }
    }

    public static class FriendSort extends WritableComparator {
        public FriendSort() {
            super(FriendBean.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            FriendBean f1 = (FriendBean) a;
            FriendBean f2 = (FriendBean) b;
            int f1Compare = f1.getFriend1().compareTo(f2.getFriend1());
            if (f1Compare == 0) {
                // 亲密度降序排序
                return -Integer.compare(f1.getFriendHot(), f2.getFriendHot());
            }
            return f1Compare;
        }
    }

    public static class FriendGroup extends WritableComparator {
        public FriendGroup() {
            super(FriendBean.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            FriendBean f1 = (FriendBean) a;
            FriendBean f2 = (FriendBean) b;
            int f1Compare = f1.getFriend1().compareTo(f2.getFriend1());
            return f1Compare;
        }
    }

    public static class FriendReducer2 extends Reducer<FriendBean, IntWritable, Text, NullWritable> {
        @Override
        protected void reduce(FriendBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable iw : values) {
                String keyOut = key.getFriend1() + "-" + key.getFriend2() + ":" + iw.get();
                context.write(new Text(keyOut), NullWritable.get());
            }
        }
    }

    public static boolean run2() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "friend2");
        job.setJarByClass(Friend.class);

        job.setMapperClass(FriendMapper2.class);
        job.setMapOutputKeyClass(FriendBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setSortComparatorClass(FriendSort.class);
        job.setGroupingComparatorClass(FriendGroup.class);

        job.setReducerClass(FriendReducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/out/FriendOut/part-r-00000"));
//        FileInputFormat.addInputPaths();
        Path outPath = new Path("/out/FriendOut2/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
            System.out.println("存在的路径已删除");
        }

        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true);
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
//        第一个求二度关系
//        if (run()) {
//            System.out.println("任务结束");
//            System.exit(0);
//        }
        if (run2()) {
            System.out.println("任务结束");
            System.exit(0);
        }
    }
}
