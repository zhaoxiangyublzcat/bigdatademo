import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MapReduce多种输入格式
 */
public class MapReduceInputFormat extends Configured implements Tool {
    public static class ScoreWritable implements WritableComparable<Object> {
        private float Chinese;
        private float Math;
        private float English;
        private float PE;
        private float CN;

        public ScoreWritable() {
        }

        public ScoreWritable(float chinese, float math, float english, float PE, float CN) {
            Chinese = chinese;
            Math = math;
            English = english;
            this.PE = PE;
            this.CN = CN;
        }

        public void set(float chinese, float math, float english, float PE, float CN) {
            Chinese = chinese;
            Math = math;
            English = english;
            this.PE = PE;
            this.CN = CN;
        }

        public float getChinese() {
            return Chinese;
        }

        public void setChinese(float chinese) {
            Chinese = chinese;
        }

        public float getMath() {
            return Math;
        }

        public void setMath(float math) {
            Math = math;
        }

        public float getEnglish() {
            return English;
        }

        public void setEnglish(float english) {
            English = english;
        }

        public float getPE() {
            return PE;
        }

        public void setPE(float PE) {
            this.PE = PE;
        }

        public float getCN() {
            return CN;
        }

        public void setCN(float CN) {
            this.CN = CN;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeFloat(Chinese);
            out.writeFloat(Math);
            out.writeFloat(English);
            out.writeFloat(PE);
            out.writeFloat(CN);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            Chinese = in.readFloat();
            Math = in.readFloat();
            English = in.readFloat();
            PE = in.readFloat();
            CN = in.readFloat();
        }
    }

    public static class ScoreInputFormat extends FileInputFormat<Text, ScoreWritable> {
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader<Text, ScoreWritable> createRecordReader(InputSplit split,
                                                                    TaskAttemptContext context) {
            return new ScoreRecordReader();
        }

        public static class ScoreRecordReader extends RecordReader<Text, ScoreWritable> {
            // 行读取器
            private LineReader in;
            // 每行数据的类型
            private Text line;
            // 自定义key类型
            private Text lineKey;
            // 自定义Value类型
            private ScoreWritable lineValue;

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
                lineValue = new ScoreWritable();
            }

            @Override
            public boolean nextKeyValue() throws IOException {
                // 每行数据
                int lineSize = in.readLine(line);
                if (lineSize == 0) {
                    return false;
                }

                String[] pieces = StringUtils.split(line.toString(), " ");
                if (pieces.length != 7) {
                    throw new IOException("行数据错误");
                }
                float Chinese, Math, English, PE, CN;
                Chinese = Float.parseFloat(pieces[2].trim());
                Math = Float.parseFloat(pieces[2].trim());
                English = Float.parseFloat(pieces[2].trim());
                PE = Float.parseFloat(pieces[2].trim());
                CN = Float.parseFloat(pieces[2].trim());

                lineKey.set(pieces[0] + "\t" + pieces[1]);
                lineValue.set(Chinese, Math, English, PE, CN);

                return true;
            }

            @Override
            public Text getCurrentKey() {
                return lineKey;
            }

            @Override
            public ScoreWritable getCurrentValue() {
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

    public static class MapReduceInputFormatMapper extends Mapper<Text, ScoreWritable, Text,
            ScoreWritable> {
        @Override
        protected void map(Text key, ScoreWritable value, Context context) throws IOException,
                InterruptedException {
            context.write(key, value);
        }
    }

    public static class MapReduceInputFormatReducer extends Reducer<Text, ScoreWritable, Text,
            Text> {
        private Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<ScoreWritable> values, Context context) throws
                IOException, InterruptedException {
            float averageNum = 0;
            float totalNum = 0;
            for (ScoreWritable value : values) {
                totalNum = value.Chinese + value.Math + value.English + value.PE + value.CN;
                averageNum = totalNum / 5;
            }
            valueOut.set("平均成绩：" + averageNum + "---总分：" + totalNum);
            context.write(key, valueOut);
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
            System.out.println("存在的路径已删除");
        }

        Job job = new Job(configuration, "MapReduceInputFormat");
        job.setJarByClass(MapReduceInputFormat.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outPath);

        job.setInputFormatClass(ScoreInputFormat.class);

        job.setMapperClass(MapReduceInputFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreWritable.class);

        job.setReducerClass(MapReduceInputFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {"/Users/hadoop/Documents/HadoopIO/in/grade.txt",
                "/Users/hadoop/Documents/HadoopIO/out/MapReduceInputformatOut/"};
        int ifExit = ToolRunner.run(new MapReduceInputFormat(), paths);
        System.exit(ifExit);
    }
}
