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

public class MeterConsumption {

    public static class MeterMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text meter = new Text();
        private DoubleWritable consumption = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("x_Timestamp")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length < 6) {
                return;
            }

            try {
                double kWh = Double.parseDouble(fields[1]);
                String meterValue = fields[5].trim();

                meter.set(meterValue);
                consumption.set(kWh);

                context.write(meter, consumption);

            } catch (Exception e) {
                // Ignore malformed rows
            }
        }
    }

    public static class MeterReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable total = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;

            for (DoubleWritable value : values) {
                sum += value.get();
            }

            total.set(sum);
            context.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Meter Wise Consumption");

        job.setJarByClass(MeterConsumption.class);

        job.setMapperClass(MeterMapper.class);
        job.setReducerClass(MeterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
