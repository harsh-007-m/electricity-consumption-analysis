import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HourlyConsumption {

    public static class HourlyMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text hour = new Text();
        private DoubleWritable consumption = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("x_Timestamp")) {
                return;
            }

            String[] fields = line.split(",");

            // Make sure row has all 6 columns
            if (fields.length < 6) {
                return;
            }

            try {
                String timestamp = fields[0];

                // Timestamp: 2020-01-01 00:00:00
                // Extract hour: 00, 01, 02, ... 23
                String hourValue = timestamp.substring(11, 13);

                double kWh = Double.parseDouble(fields[1]);

                hour.set(hourValue);
                consumption.set(kWh);

                context.write(hour, consumption);

            } catch (Exception e) {
                // Ignore malformed rows
            }
        }
    }

    public static class HourlyReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable totalConsumption = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;

            for (DoubleWritable value : values) {
                sum += value.get();
            }

            totalConsumption.set(sum);

            context.write(key, totalConsumption);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: HourlyConsumption <input path> <output path>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "24 Hour Load Profile");

        job.setJarByClass(HourlyConsumption.class);

        job.setMapperClass(HourlyMapper.class);
        job.setReducerClass(HourlyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
