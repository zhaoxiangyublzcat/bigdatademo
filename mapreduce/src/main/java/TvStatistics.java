import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author blz
 */
public class TvStatistics extends Configured implements Tool {

    /**
     * 自定义TvWritable类型进行封装相关电视剧的信息
     */
    public static class TvWritable implements WritableComparable<Object> {
        // 电视剧的 播放量 收藏数 评论数 踩数 赞数
        private int playNum, collect, comment, dislike, like;

        int getPlayNum() {
            return playNum;
        }

        public void setPlayNum(int playNum) {
            this.playNum = playNum;
        }

        int getCollect() {
            return collect;
        }

        public void setCollect(int collect) {
            this.collect = collect;
        }

        int getComment() {
            return comment;
        }

        public void setComment(int comment) {
            this.comment = comment;
        }

        int getDislike() {
            return dislike;
        }

        public void setDislike(int dislike) {
            this.dislike = dislike;
        }

        int getLike() {
            return like;
        }

        public void setLike(int like) {
            this.like = like;
        }


        public void set(int playNum, int collect, int comment, int dislike, int like) {
            this.playNum = playNum;
            this.collect = collect;
            this.comment = comment;
            this.dislike = dislike;
            this.like = like;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.playNum);
            out.writeInt(this.collect);
            out.writeInt(this.comment);
            out.writeInt(this.dislike);
            out.writeInt(this.like);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.playNum = in.readInt();
            this.collect = in.readInt();
            this.comment = in.readInt();
            this.dislike = in.readInt();
            this.like = in.readInt();
        }

        @Override
        public String toString() {
            return playNum + "\t" + collect + "\t" + comment + "\t" + dislike + "\t" + like;
        }
    }

    public static class TvInputFormat extends FileInputFormat<Text, TvWritable> {
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader<Text, TvWritable> createRecordReader(InputSplit split,
                                                                 TaskAttemptContext context) {
            return new TvRecordReader();
        }

        public static class TvRecordReader extends RecordReader<Text, TvWritable> {
            /**
             * 行读取器
             */
            private LineReader in;
            /**
             * 行类型
             */
            private Text line;
            /**
             * 自定义key类型
             */
            private Text lineKey;
            /**
             * 自定义value类型
             */
            private TvWritable lineValue;

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws
                    IOException {
                FileSplit fileSplit = (FileSplit) split;
                Configuration configuration = context.getConfiguration();
                Path filePath = fileSplit.getPath();
                FileSystem fileSystem = filePath.getFileSystem(configuration);
                FSDataInputStream fsDataInputStream = fileSystem.open(filePath);

                in = new LineReader(fsDataInputStream, configuration);
                line = new Text();
                lineKey = new Text();
                lineValue = new TvWritable();
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                int lineSize = in.readLine(line);
                if (lineSize == 0) {
                    return false;
                }

                String[] lineValues = line.toString().split("\t");
                if (lineValues.length != 7) {
                    throw new IOException("无效的记录值");
                }

                String name, playFrom;
                int playNum, collect, comment, dislike, like;
                name = lineValues[0];
                playFrom = lineValues[1];
                playNum = Integer.parseInt(lineValues[2]);
                collect = Integer.parseInt(lineValues[3]);
                comment = Integer.parseInt(lineValues[4]);
                dislike = Integer.parseInt(lineValues[5]);
                like = Integer.parseInt(lineValues[6]);

                lineKey.set(name + "\t" + playFrom);
                lineValue.set(playNum, collect, comment, dislike, like);

                return true;
            }

            @Override
            public Text getCurrentKey() {
                return lineKey;
            }

            @Override
            public TvWritable getCurrentValue() {
                return lineValue;
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public void close() {

            }
        }
    }

    public static class TvStatisticsMapper extends Mapper<Text, TvWritable, Text, TvWritable> {
        @Override
        protected void map(Text key, TvWritable value, Context context) throws IOException,
                InterruptedException {
            context.write(key, value);
        }
    }

    public static class TvStatisticsReducer extends Reducer<Text, TvWritable, Text, Text> {
        private MultipleOutputs multipleOutputs = null;
        private Text keyOut = new Text();
        private TvWritable valueOut = new TvWritable();

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<TvWritable> values, Context context) throws
                IOException, InterruptedException {
            //名称+"\t"+网址
            String[] keyLineValues = key.toString().split("\t");
            String name = keyLineValues[0];
            String address = keyLineValues[1];

            int playNum = 0, collect = 0, comment = 0, dislike = 0, like = 0;
            for (TvWritable value : values) {
                playNum += value.getPlayNum();
                collect += value.getCollect();
                comment += value.getComment();
                dislike += value.getDislike();
                like += value.getLike();
            }

            valueOut.set(playNum, collect, comment, dislike, like);

            if (address.equals("1")) {
                keyOut.set(address + "--" + "优酷");
                multipleOutputs.write(keyOut, valueOut, "youku");
            } else if (address.equals("2")) {
                keyOut.set(address + "--" + "搜狐");
                multipleOutputs.write(keyOut, valueOut, "souhu");
            } else if (address.equals("3")) {
                keyOut.set(address + "--" + "土豆");
                multipleOutputs.write(keyOut, valueOut, "tudou");
            } else if (address.equals("4")) {
                keyOut.set(address + "--" + "爱奇艺");
                multipleOutputs.write(keyOut, valueOut, "aiqiyi");
            } else {
                keyOut.set(address + "--" + "迅雷");
                multipleOutputs.write(keyOut, valueOut, "xunlei");
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Path outPath = new Path(otherArgs[otherArgs.length - 1]);

        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的输出路径已删除");
        }

        Job job = new Job(configuration, "TvStatistics");
        job.setJarByClass(TvStatistics.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outPath);

        job.setInputFormatClass(TvInputFormat.class);

        job.setMapperClass(TvStatisticsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TvWritable.class);

        job.setReducerClass(TvStatisticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TvWritable.class);

        MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, Text
                .class);
        MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class, Text.class, Text
                .class);
        MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class, Text.class, Text
                .class);
        MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class, Text.class, Text
                .class);
        MultipleOutputs.addNamedOutput(job, "xunlei", TextOutputFormat.class, Text.class, Text
                .class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/tvplay.txt",
                "/Users/hadoop/Documents/HadoopIO/out/TvStatistics/"};
        int ifExit = ToolRunner.run(new TvStatistics(), paths);
        System.exit(ifExit);
    }
}
