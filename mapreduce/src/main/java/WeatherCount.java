import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class WeatherCount {
    /**
     * 自定义weather bean类
     * 2019012839
     */
    public static class Weather implements WritableComparable<Weather> {
        private int year;
        private int month;
        private int day;
        private int temperature;

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getMonth() {
            return month;
        }

        public void setMonth(int month) {
            this.month = month;
        }

        public int getDay() {
            return day;
        }

        public void setDay(int day) {
            this.day = day;
        }

        public int getTemperature() {
            return temperature;
        }

        public void setTemperature(int temperature) {
            this.temperature = temperature;
        }

        @Override
        public int compareTo(Weather o) {
            int yearCom = Integer.compare(this.year, o.getYear());
            if (yearCom == 0) {
                int monthCom = Integer.compare(this.month, o.getMonth());
                if (monthCom == 0) {
                    int dayCom = Integer.compare(this.day, o.getDay());
                    if (dayCom == 0) {
                        return Integer.compare(this.temperature, o.getTemperature());
                    }
                    return dayCom;
                }
                return monthCom;
            }
            return yearCom;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(year);
            dataOutput.writeInt(month);
            dataOutput.writeInt(day);
            dataOutput.writeInt(temperature);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.year = dataInput.readInt();
            this.month = dataInput.readInt();
            this.day = dataInput.readInt();
            this.temperature = dataInput.readInt();
        }
    }

    /**
     * 重写mapper类
     */
    public static class WeatherCountMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = StringUtils.split(value.toString(), '\t');
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar cal = Calendar.getInstance();
            try {
                cal.setTime(format.parse(lines[0]));
                Weather weather = new Weather();
                weather.setYear(cal.get(Calendar.YEAR));
                weather.setMonth(cal.get(Calendar.MONTH) + 1);
                weather.setDay(cal.get(Calendar.DAY_OF_MONTH));

                int temperature = Integer.parseInt(lines[1].substring(0, lines[1].indexOf("c")));
                weather.setTemperature(temperature);

                context.write(weather, new IntWritable(temperature));
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    /******************************************shuffle
     /**
     * 自定义分区partiton类
     */
    public static class WeatherPartitioner extends HashPartitioner<Weather, IntWritable> {
        @Override
        public int getPartition(Weather key, IntWritable value, int numReduceTasks) {
            return (key.getYear() - 1949) % numReduceTasks;
        }
    }

    /**
     * 自定义排序方法
     */
    public static class WeatherSort extends WritableComparator {
        protected WeatherSort() {
            super(Weather.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Weather w1 = (Weather) a;
            Weather w2 = (Weather) b;

            int yearCom = Integer.compare(w1.getYear(), w2.getYear());
            if (yearCom == 0) {
                int monthCom = Integer.compare(w1.getMonth(), w2.getMonth());
                if (monthCom == 0) {
                    return -Integer.compare(w1.getTemperature(), w2.getTemperature());
                }
                return monthCom;
            }
            return yearCom;
        }
    }

    public static class WeatherGroup extends WritableComparator {
        protected WeatherGroup() {
            super(Weather.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Weather w1 = (Weather) a;
            Weather w2 = (Weather) b;

            int yearCom = Integer.compare(w1.getYear(), w2.getYear());
            if (yearCom == 0) {
                return Integer.compare(w1.getMonth(), w2.getMonth());
            }
            return yearCom;
        }
    }
    //shuffle******************************************

    /**
     * 自定义reducer类
     */
    public static class WeatherCountReducer extends Reducer<Weather, IntWritable, Text, NullWritable> {
        @Override
        protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int flag = 0;
            for (IntWritable iw : values) {
                flag++;
                if (flag > 2) {
                    break;
                }

                String line = key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "-" + key.getMonth() + "    " + iw.get();
                context.write(new Text(line), NullWritable.get());
            }
        }
    }

    public static boolean run(String inputPath, String outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "weathercount");
        job.setJarByClass(WeatherCount.class);
        // map阶段
        job.setMapperClass(WeatherCountMapper.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(IntWritable.class);

        // shuffler
        job.setPartitionerClass(WeatherPartitioner.class);
        job.setSortComparatorClass(WeatherSort.class);
        job.setGroupingComparatorClass(WeatherGroup.class);
        job.setNumReduceTasks(3);

        // reducer
        job.setReducerClass(WeatherCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        Path outPath = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
            System.out.println("存在的路径已删除");
        }
        FileOutputFormat.setOutputPath(job, outPath);

        boolean ifExit = job.waitForCompletion(true);
        return ifExit;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        boolean ifExit = run(args[0], args[1]);
        if (ifExit) {
            System.out.println("任务结束");
            System.exit(0);
        }
    }
}
