import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YearlyMeterConsumption {

    public static class MeterMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text meter = new Text();
        private Text consumption = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty() || line.startsWith("x_Timestamp")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length < 6) {
                return;
            }

            try {
                double kWh = Double.parseDouble(fields[1]);
                String meterId = fields[5].trim();

                meter.set(meterId);
                consumption.set(String.valueOf(kWh));

                context.write(meter, consumption);

            } catch (NumberFormatException e) {
                // Ignore malformed records
            }
        }
    }

    public static class MeterReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            double total = 0.0;

            for (Text value : values) {
                total += Double.parseDouble(value.toString());
            }

            result.set(String.valueOf(total));

            context.write(key, result);
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: YearlyMeterConsumption <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Yearly Meter Consumption"
        );

        job.setJarByClass(
            YearlyMeterConsumption.class
        );

        job.setMapperClass(
            MeterMapper.class
        );

        job.setReducerClass(
            MeterReducer.class
        );

        job.setOutputKeyClass(
            Text.class
        );

        job.setOutputValueClass(
            Text.class
        );

        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
