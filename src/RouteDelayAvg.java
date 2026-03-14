import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class RouteDelayAvg {

    public static class RouteMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.startsWith("FL_DATE")) return;

            String[] fields = line.split(",", -1);
            if (fields.length < 20) return;

            try {
                String origin = fields[6].trim().replace("\"", "");
                String dest = fields[8].trim().replace("\"", "");
                String delayStr = fields[12].trim().replace("\"", "");

                if (!origin.isEmpty() && !dest.isEmpty() && !delayStr.isEmpty()) {
                    double delay = Double.parseDouble(delayStr);
                    String route = origin + " -> " + dest;

                    context.write(new Text(route), new DoubleWritable(delay));
                }
            } catch (NumberFormatException e) {
                // skip invalid delay
            }
        }
    }

    public static class RouteReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double total = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                total += val.get();
                count++;
            }

            double avg = total / count;

            if (avg > 15) {
                context.write(key, new DoubleWritable(Math.round(avg * 100.0) / 100.0));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Route Delay Average");

        job.setJarByClass(RouteDelayAvg.class);
        job.setMapperClass(RouteMapper.class);
        job.setReducerClass(RouteReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}