import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YearComparisonReduceJoin {

    public static class Mapper2020
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().trim().split("\\s+");

            if (parts.length >= 2) {
                context.write(
                    new Text(parts[0]),
                    new Text("2020|" + parts[1])
                );
            }
        }
    }

    public static class Mapper2021
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().trim().split("\\s+");

            if (parts.length >= 2) {
                context.write(
                    new Text(parts[0]),
                    new Text("2021|" + parts[1])
                );
            }
        }
    }

    public static class JoinReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            String consumption2020 = null;
            String consumption2021 = null;

            for (Text value : values) {

                String[] parts =
                    value.toString().split("\\|");

                if (parts[0].equals("2020")) {
                    consumption2020 = parts[1];
                }

                if (parts[0].equals("2021")) {
                    consumption2021 = parts[1];
                }
            }

            if (consumption2020 != null &&
                consumption2021 != null) {

                context.write(
                    key,
                    new Text(
                        "2020=" + consumption2020 +
                        "\t2021=" + consumption2021
                    )
                );
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Reduce Side Join 2020 2021"
        );

        job.setJarByClass(
            YearComparisonReduceJoin.class
        );

        MultipleInputs.addInputPath(
            job,
            new Path(args[0]),
            TextInputFormat.class,
            Mapper2020.class
        );

        MultipleInputs.addInputPath(
            job,
            new Path(args[1]),
            TextInputFormat.class,
            Mapper2021.class
        );

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[2])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
