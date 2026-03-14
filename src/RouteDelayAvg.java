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
                String origin = fields[6].trim().replace("\"", "");    // ORIGIN (col G)
                String dest   = fields[8].trim().replace("\"", "");    // DEST (col I)
                String delayStr = fields[12].trim().replace("\"", ""); // DEP_DELAY (col M)

                if (!origin.isEmpty() && !dest.isEmpty() && !delayStr.isEmpty()) {
                    double delay = Double.parseDouble(delayStr);
                    String route = origin + " -> " + dest;
                    context.write(new Text(route), new DoubleWritable(delay));
                }
            } catch (NumberFormatException e) {
                // skip
            }
        }
    }