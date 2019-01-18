package SchoolDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow {
    /**
     * 自定义序列化类型
     */
    public static class FlowWriter implements WritableComparable<Object> {
        private double upFlow;
        private double loadFlow;

        public FlowWriter() {
        }

        public double getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(double upFlow) {
            this.upFlow = upFlow;
        }

        public double getLoadFlow() {
            return loadFlow;
        }

        public void setLoadFlow(double loadFlow) {
            this.loadFlow = loadFlow;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(upFlow);
            dataOutput.writeDouble(loadFlow);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.upFlow = dataInput.readDouble();
            this.loadFlow = dataInput.readDouble();
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }
    }

    public static class FlowInputFormat extends FileInputFormat<Text, FlowWriter> {
        @Override
        public RecordReader<Text, FlowWriter> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
            return new FlowRecordReader();
        }

        public static class FlowRecordReader extends RecordReader<Text, FlowWriter> {
            // 行读取器
            private LineReader in;
            // 行类型
            private Text line;
            // key类型
            private Text key;
            // value类型
            private FlowWriter value;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
                FileSplit split = (FileSplit) inputSplit;
                Configuration conf = taskAttemptContext.getConfiguration();
                Path path = split.getPath();
                FileSystem fileSystem = path.getFileSystem(conf);
                FSDataInputStream is = fileSystem.open(path);

                in = new LineReader(is, conf);
                line = new Text();
                key = new Text();
                value = new FlowWriter();
            }

            @Override
            public boolean nextKeyValue() throws IOException {
                int lineSize = in.readLine(line);
                if (lineSize == 0) {
                    return false;
                }

                String[] lines = line.toString().split("\t");
                String phoneNum;
                double load, up;
                phoneNum = lines[1];
                load = Double.parseDouble(lines[lines.length - 2]);
                up = Double.parseDouble(lines[lines.length - 1]);

                key.set(phoneNum);
                value.setLoadFlow(load);
                value.setUpFlow(up);
                return true;
            }

            @Override
            public Text getCurrentKey() {
                return key;
            }

            @Override
            public FlowWriter getCurrentValue() {
                return value;
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

    public static class FlowMapper extends Mapper<Text, FlowWriter, Text, FlowWriter> {
        @Override
        protected void map(Text key, FlowWriter value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class FlowReducer extends Reducer<Text, FlowWriter, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<FlowWriter> values, Context context) throws IOException, InterruptedException {
            double upSum = 0, loadSum = 0;
            for (FlowWriter value : values) {
                upSum = value.getUpFlow();
                loadSum = value.getLoadFlow();
            }

            context.write(key, new Text("下载：" + loadSum + " 上传：" + upSum));
        }
    }

    public int run(String inPathString, String outPathString) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);
        Path outPath = new Path(outPathString);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        Job job = new Job(conf, "phoneFlow");
        job.setJarByClass(Flow.class);

        job.setInputFormatClass(FlowInputFormat.class);

        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowWriter.class);

        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inPathString));
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        int ifExit = new Flow().run(args[0], args[1]);
        System.exit(ifExit);
    }
}
