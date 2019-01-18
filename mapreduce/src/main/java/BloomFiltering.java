import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * bloomfilter实现join
 */
public class BloomFiltering extends Configured implements Tool {

    public static class BloomFilteringMapper extends Mapper<Object, Text, Text, Text> {
        private BloomFilter filter = new BloomFilter(10000, 6, Hash.MURMUR_HASH);

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader reader = null;
            String line = null;
            // 返回缓存的地址
            Path[] cacheFilePath = context.getLocalCacheFiles();
            for (Path filePath : cacheFilePath) {
                String pathStr = filePath.toString();
                reader = new BufferedReader(new FileReader(pathStr));
                while ((line = reader.readLine()) != null) {
                    String[] records = StringUtils.split(line.toString(), "\\s+");
                    if (records != null) {
                        // stationID
                        filter.add(new Key(records[0].getBytes()));
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();

            if (pathName.endsWith("records-semi.txt")) {
                String[] lineValues = StringUtils.split(value.toString(), "\\s+");
                if (lineValues.length != 3) {
                    return;
                }
                // 通过filter过滤大表数据
                if (filter.membershipTest(new Key(lineValues[0].getBytes()))) {
                    keyOut.set(lineValues[0]);
                    valueOut.set("records-semi.txt" + lineValues[1] + "\t" + lineValues[2]);
                    context.write(keyOut, valueOut);
                }
            } else if (pathName.endsWith("station.txt")) {
                String[] lineValues = StringUtils.split(value.toString(), "\\s+");
                if (lineValues.length != 2) {
                    return;
                }
                if (filter.membershipTest(new Key(lineValues[0].getBytes()))) {
                    keyOut.set(lineValues[0]);
                    valueOut.set("station.txt" + lineValues[1]);
                    context.write(keyOut, valueOut);
                }
            }
        }
    }

    public static class BloomFilteringReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> leftList = new ArrayList<String>();
        private List<String> rightList = new ArrayList<String>();

        private Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            leftList.clear();
            rightList.clear();

            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.startsWith("station.txt")) {
                    leftList.add(valueStr.replaceFirst("station.txt", ""));
                } else if (valueStr.startsWith("records‐semi.txt")) {
                    rightList.add(valueStr.replaceFirst("records‐semi.txt", ""));
                }
            }

            for (String left : leftList) {
                for (String right : rightList) {
                    valueOut.set(left + "\t" + right);
                    context.write(key, valueOut);
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        Path outPath = new Path(otherArgs[otherArgs.length - 1]);

        FileSystem fileSystem = outPath.getFileSystem(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
            System.out.println("存在的路径已删除");
        }

        Job job = new Job(configuration, "BloomFiltering");
        job.setJarByClass(BloomFiltering.class);

        job.addCacheFile(new URI(otherArgs[0]));

        job.setMapperClass(BloomFilteringMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(BloomFilteringReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 1; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = {args[0], args[1], args[2]};
        int ifExit = ToolRunner.run(new BloomFiltering(), paths);
        System.exit(ifExit);
    }
}
