import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class AirlineDelayCount {

    public static class DelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("FL_DATE")) return; // skip header

            String[] fields = line.split(",", -1);
            if (fields.length < 20) return;

            try {
                String airline = fields[3].trim().replace("\"", "");  // AIRLINE_CODE (col D)
                String depDelayStr = fields[12].trim().replace("\"", ""); // DEP_DELAY (col M)

                if (!airline.isEmpty() && !depDelayStr.isEmpty()) {
                    double delay = Double.parseDouble(depDelayStr);
                    if (delay > 0) {
                        context.write(new Text(airline), new IntWritable(1));
                    }
                }
            } catch (NumberFormatException e) {
                // skip bad rows
            }
        }
    }

    public static class DelayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable val : values) total += val.get();
            context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Airline Delay Count");
        job.setJarByClass(AirlineDelayCount.class);
        job.setMapperClass(DelayMapper.class);
        job.setReducerClass(DelayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}