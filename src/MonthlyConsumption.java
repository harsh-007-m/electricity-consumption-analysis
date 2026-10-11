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

public class MonthlyConsumption {

    public static class MonthlyMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text month = new Text();
        private DoubleWritable consumption = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("x_Timestamp")) {
                return;
            }

            String[] fields = line.split(",");

            // Make sure the row has all 6 columns
            if (fields.length < 6) {
                return;
            }

            try {
                // Timestamp format: 2020-01-01 00:00:00
                String timestamp = fields[0];

                // Extract YYYY-MM
                String monthValue = timestamp.substring(0, 7);

                // t_kWh is column 2
                double kWh = Double.parseDouble(fields[1]);

                month.set(monthValue);
                consumption.set(kWh);

                context.write(month, consumption);

            } catch (Exception e) {
                // Ignore malformed rows
            }
        }
    }

    public static class MonthlyReducer
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
                "Usage: MonthlyConsumption <input path> <output path>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Monthly Electricity Consumption");

        job.setJarByClass(MonthlyConsumption.class);

        job.setMapperClass(MonthlyMapper.class);
        job.setReducerClass(MonthlyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
