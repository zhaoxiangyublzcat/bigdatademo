import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class PageRank {
    public static class Node {
        private double pageRank = 1.0;
        private String[] adNodeNames;
        // 分隔符
        private static final char fieldSeparator = '\t';

        public double getPageRank() {
            return pageRank;
        }

        public Node setPageRank(double pageRank) {
            this.pageRank = pageRank;
            return this;
        }

        public String[] getAdNodeNames() {
            return adNodeNames;
        }

        public Node setAdNodeNames(String[] adNodeNames) {
            this.adNodeNames = adNodeNames;
            return this;
        }

        public boolean containsAdNodeNames() {
            return adNodeNames != null && adNodeNames.length > 0;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(pageRank);

            if (getAdNodeNames() != null) {
                sb.append(fieldSeparator).append(StringUtils.join(getAdNodeNames(), fieldSeparator));
            }

            return sb.toString();
        }

        // 1.0  A    B   C
        public static Node fromMR(String value) throws IOException {
            String[] parts = StringUtils.split(value, fieldSeparator);
            if (parts.length < 1) {
                throw new IOException("parts length not enough:" + parts.length);
            }

            Node node = new Node().setPageRank(Double.parseDouble(parts[0]));
            if (parts.length > 1) {
                node.setAdNodeNames(Arrays.copyOfRange(parts, 1, parts.length));

            }
            return node;
        }
    }

    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int runCount = context.getConfiguration().getInt("pagerank", 1);
            // a    b   c
            String page = key.toString();

            Node node = null;
            if (runCount == 1) {
                node = Node.fromMR("1.0" + "\t" + value.toString());
            } else {
                node = Node.fromMR(value.toString());
            }

            context.write(new Text(page), new Text(node.toString()));

            if (node.containsAdNodeNames()) {
                double outValue = node.getPageRank() / node.getAdNodeNames().length;
                for (int i = 0; i < node.getAdNodeNames().length; i++) {
                    String outputPage = node.getAdNodeNames()[i];
                    context.write(new Text(outputPage), new Text(outValue + ""));
                }
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            Node sourceNode = null;
            for (Text t : values) {
                Node node = Node.fromMR(t.toString());
                // A:1.0    B   C
                if (node.containsAdNodeNames()) {
                    sourceNode = node;
                } else {
                    sum = sum + node.getPageRank();
                }
            }

            double newPageRank = (0.15 / 4.0) + (0.85 * sum);
            double d = newPageRank - sourceNode.getPageRank();
            int j = (int) (d * 1000.0);
            j = Math.abs(j);
//            context.getCounter().increment(i);

            sourceNode.setPageRank(newPageRank);
            context.write(key, new Text(sourceNode.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        double d = 0.001;
        int i = 0;
        while (true) {
            i++;
            //记录计算次数
            conf.setInt("pagerank", i);

            FileSystem fs = FileSystem.get(conf);

            Job job = Job.getInstance(conf);
            job.setJobName("PageRank" + i);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(PageRankReducer.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            Path inputPath = new Path("/in/pagerank/pagerank.txt");
            if (i > 1) {
                inputPath = new Path("/out/pagerank/PageRank" + (i - 1));
            }
            FileInputFormat.addInputPath(job, inputPath);

            Path outPath = new Path("/out/pagerank/PageRank" + i);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            boolean ifExit = job.waitForCompletion(true);
            if (ifExit) {
                System.out.println("successful");
//                long sum = job.getCounters().findCounter().getValue();
//                double avgd = sum / 4000.0;
//                if (avgd < d) {
//                    break;
//                }
            }

        }
    }
}
