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
import java.util.List;

/**
 * TVStatistics2 class function description
 * 数据格式：电视剧 电视剧播放网站 播放量 收藏数 评论数 踩数 赞数
 *
 * @author hadoop
 */
public class TvStatistics2 extends Configured implements Tool {

    /**
     * 自定义输入的数据类型
     */
    public static class TvInputData implements WritableComparable<Object> {
        private int playNum;
        private int collectNum;
        private int commentNum;
        private int dislikeNum;
        private int likeNum;

        public TvInputData() {
        }

        public void set(int playNum, int collectNum, int commentNum, int dislikeNum, int likeNum) {
            this.playNum = playNum;
            this.collectNum = collectNum;
            this.commentNum = commentNum;
            this.dislikeNum = dislikeNum;
            this.likeNum = likeNum;
        }

        public int getPlayNum() {
            return playNum;
        }

        public void setPlayNum(int playNum) {
            this.playNum = playNum;
        }

        public int getCollectNum() {
            return collectNum;
        }

        public void setCollectNum(int collectNum) {
            this.collectNum = collectNum;
        }

        public int getCommentNum() {
            return commentNum;
        }

        public void setCommentNum(int commentNum) {
            this.commentNum = commentNum;
        }

        public int getDislikeNum() {
            return dislikeNum;
        }

        public void setDislikeNum(int dislikeNum) {
            this.dislikeNum = dislikeNum;
        }

        public int getLikeNum() {
            return likeNum;
        }

        public void setLikeNum(int likeNum) {
            this.likeNum = likeNum;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(playNum);
            out.writeInt(collectNum);
            out.writeInt(commentNum);
            out.writeInt(dislikeNum);
            out.writeInt(likeNum);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            playNum = in.readInt();
            collectNum = in.readInt();
            commentNum = in.readInt();
            dislikeNum = in.readInt();
            likeNum = in.readInt();
        }
    }


    public static class TvInputFormat extends FileInputFormat<Text, TvInputData> {

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            return super.getSplits(job);
        }

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader<Text, TvInputData> createRecordReader(InputSplit split, TaskAttemptContext context) throws
                IOException, InterruptedException {
            return new TvRecordReader();
        }

        public static class TvRecordReader extends RecordReader<Text, TvInputData> {
            private LineReader in;
            private Text lineKey;
            private TvInputData lineValue;
            private Text line;


            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
                    InterruptedException {
                FileSplit split1 = (FileSplit) split;
                Configuration job = context.getConfiguration();
                Path file = split1.getPath();

                // 打开文件进行读取
                FileSystem fileSystem = file.getFileSystem(job);
                FSDataInputStream fileIn = fileSystem.open(file);
                in = new LineReader(fileIn, job);
                lineKey = new Text();
                lineValue = new TvInputData();
                line = new Text();
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                int lineSize = in.readLine(line);
                if (lineSize == 0) {
                    return false;
                }

                String[] lines = line.toString().split("\t");
                if (lines.length != 7) {
                    throw new IOException("Invalid record received");
                }

                lineKey.set(lines[0] + "\t" + lines[1]);
                lineValue.set(Integer.parseInt(lines[2]),
                        Integer.parseInt(lines[3]),
                        Integer.parseInt(lines[4]),
                        Integer.parseInt(lines[5]),
                        Integer.parseInt(lines[6]));
                return true;
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return lineKey;
            }

            @Override
            public TvInputData getCurrentValue() throws IOException, InterruptedException {
                return lineValue;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                if (in != null) {
                    in.close();
                }
            }
        }
    }

    /**
     * 自定义Mapper类，直接写入Reducer
     */
    public static class TvStatistics2Mapper extends Mapper<Text, TvInputData, Text, TvInputData> {
        @Override
        protected void map(Text key, TvInputData value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class TvStatistics2Reducer extends Reducer<Text, TvInputData, Text, Text> {
        private Text keyOut = null;
        private Text valueOut = null;
        private MultipleOutputs<Text, Text> outputs = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<TvInputData> values, Context context) throws IOException,
                InterruptedException {
            int playNum = 0;
            int collectNum = 0;
            int commentNum = 0;
            int dislikeNum = 0;
            int likeNum = 0;

            for (TvInputData value : values) {
                playNum += value.getPlayNum();
                collectNum += value.getPlayNum();
                commentNum += value.getPlayNum();
                dislikeNum += value.getPlayNum();
                likeNum += value.getPlayNum();
            }

            String[] keys = key.toString().split("\t");
            String website = keys[1];

            keyOut.set(keys[0]);
            valueOut.set(playNum + "\t" + collectNum +
                    "\t" + commentNum + "\t" + dislikeNum + "\t" + likeNum);

            switch (website) {
                case "1":
                    website = "qq";
                    break;
                case "2":
                    website = "souhu";
                    break;
                case "3":
                    website = "leshi";
                    break;
                case "4":
                    website = "youku";
                    break;
                default:
                    website = "aiqiyi";
                    break;
            }
            outputs.write(website, keyOut, valueOut);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
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
        }

        Job job = new Job(configuration, "TvStatistics");
        job.setJarByClass(TvStatistics2.class);

        job.setInputFormatClass(TvInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, outPath);

        MultipleOutputs.addNamedOutput(job, "qq", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "leshi", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class, Text.class, Text.class);

        job.setMapperClass(TvStatistics2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TvInputData.class);

        job.setReducerClass(TvStatistics2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/tvplay.txt",
                "/Users/hadoop/Documents/HadoopIO/out/TvStatistics2OutPut"};
        int ifExit = ToolRunner.run(new TvStatistics2(), paths);
        System.exit(ifExit);
    }
}
