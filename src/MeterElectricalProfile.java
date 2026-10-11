import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeterElectricalProfile {

    public static class ProfileMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text meter = new Text();
        private Text values = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("x_Timestamp")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length < 6) {
                return;
            }

            try {
                String meterValue = fields[5].trim();
                double voltage = Double.parseDouble(fields[2]);
                double current = Double.parseDouble(fields[3]);

                meter.set(meterValue);

                // voltage,current,count
                values.set(voltage + "," + current + ",1");

                context.write(meter, values);

            } catch (Exception e) {
                // Ignore malformed rows
            }
        }
    }

    public static class ProfileReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            double voltageSum = 0.0;
            double currentSum = 0.0;
            long count = 0;

            for (Text value : values) {

                String[] parts = value.toString().split(",");

                voltageSum += Double.parseDouble(parts[0]);
                currentSum += Double.parseDouble(parts[1]);
                count += Long.parseLong(parts[2]);
            }

            double avgVoltage = voltageSum / count;
            double avgCurrent = currentSum / count;

            result.set(avgVoltage + "," + avgCurrent);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,
                "Meter Electrical Profile");

        job.setJarByClass(MeterElectricalProfile.class);

        job.setMapperClass(ProfileMapper.class);
        job.setReducerClass(ProfileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

