import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountHBaseMain extends Configured implements Tool {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "Master");
		//端口号
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.master", "Master:60000");
	}

	public static class WordCountMapperHbase extends Mapper<Object, Text, ImmutableBytesWritable,
			IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);

		@Override
		protected void map(Object key, Text value, Context context) throws IOException,
				InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				context.write(new ImmutableBytesWritable(Bytes.toBytes(new Text(itr.nextToken())
						.toString())), ONE);
			}
		}
	}

	public static class WordCountReduceHbase extends TableReducer<ImmutableBytesWritable,
			IntWritable, ImmutableBytesWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context
				context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String
					.valueOf(sum)));
			context.write(key, put);
		}
	}


	public static String creatTable(String tableName) throws IOException {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		if (hBaseAdmin.tableExists(tableName)) {
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
		}

		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("content");
		descriptor.addFamily(hColumnDescriptor);
		hBaseAdmin.createTable(descriptor);

		return tableName;
	}

	public int run(String[] strings) throws Exception {
		String string = creatTable("wordcount");

		Job job = new Job(configuration, "WordCountHbaseMain");
		job.setJarByClass(WordCountHBaseMain.class);

		job.setMapperClass(WordCountMapperHbase.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		TableMapReduceUtil.initTableReducerJob(string, WordCountReduceHbase.class, job, null,
				null, null, null);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);

		FileInputFormat.addInputPaths(job, strings[0]);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] path = {"/Users/hadoop/Documents/HadoopIO/in/wordCount.txt"};
		int ifExit = ToolRunner.run(new WordCountHBaseMain(), path);
		System.exit(ifExit);
	}
}
