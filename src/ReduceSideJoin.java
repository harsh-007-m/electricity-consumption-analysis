import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoin {

    // Mapper for meter consumption dataset
    public static class ConsumptionMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text meter = new Text();
        private Text data = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split("\\s+");

            if (fields.length < 2) {
                return;
            }

            meter.set(fields[0]);
            data.set("C|" + fields[1]);

            context.write(meter, data);
        }
    }

    // Mapper for electrical profile dataset
    public static class ProfileMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text meter = new Text();
        private Text data = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split("\\s+");

            if (fields.length < 2) {
                return;
            }

            String[] profile = fields[1].split(",");

            if (profile.length < 2) {
                return;
            }

            meter.set(fields[0]);

            data.set(
                "E|" + profile[0] + "|" + profile[1]
            );

            context.write(meter, data);
        }
    }

    // Reducer performs the actual join
    public static class JoinReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            String totalConsumption = null;
            String avgVoltage = null;
            String avgCurrent = null;

            for (Text value : values) {

                String[] parts =
                    value.toString().split("\\|");

                if (parts[0].equals("C")) {

                    totalConsumption = parts[1];

                } else if (parts[0].equals("E")) {

                    avgVoltage = parts[1];
                    avgCurrent = parts[2];
                }
            }

            // Output only when both datasets contain this meter
            if (totalConsumption != null &&
                avgVoltage != null &&
                avgCurrent != null) {

                result.set(
                    totalConsumption + "\t" +
                    avgVoltage + "\t" +
                    avgCurrent
                );

                context.write(key, result);
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 3) {

            System.err.println(
                "Usage: ReduceSideJoin " +
                "<consumption> <profile> <output>"
            );

            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Reduce Side Join"
        );

        job.setJarByClass(
            ReduceSideJoin.class
        );

        /*
         * Explicitly assign different mappers
         * to the two input datasets.
         */
        MultipleInputs.addInputPath(
            job,
            new Path(args[0]),
            TextInputFormat.class,
            ConsumptionMapper.class
        );

        MultipleInputs.addInputPath(
            job,
            new Path(args[1]),
            TextInputFormat.class,
            ProfileMapper.class
        );

        job.setReducerClass(
            JoinReducer.class
        );

        job.setMapOutputKeyClass(
            Text.class
        );

        job.setMapOutputValueClass(
            Text.class
        );

        job.setOutputKeyClass(
            Text.class
        );

        job.setOutputValueClass(
            Text.class
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[2])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
