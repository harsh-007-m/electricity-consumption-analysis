import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin {

    public static class JoinMapper
            extends Mapper<Object, Text, Text, Text> {

        private Map<String, String> data2021 =
                new HashMap<>();

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void setup(Context context)
                throws IOException {

            Configuration conf =
                    context.getConfiguration();

            Path cacheFile =
                    new Path(conf.get("join.file"));

            FileSystem fs =
                    cacheFile.getFileSystem(conf);

            BufferedReader reader =
                    new BufferedReader(
                        new InputStreamReader(
                            fs.open(cacheFile)
                        )
                    );

            String line;

            while ((line = reader.readLine()) != null) {

                String[] parts =
                        line.trim().split("\\s+");

                if (parts.length >= 2) {

                    data2021.put(
                        parts[0],
                        parts[1]
                    );
                }
            }

            reader.close();
        }

        @Override
        public void map(Object key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line =
                    value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts =
                    line.split("\\s+");

            if (parts.length < 2) {
                return;
            }

            String meter = parts[0];
            String consumption2020 = parts[1];

            String consumption2021 =
                    data2021.get(meter);

            if (consumption2021 != null) {

                outputKey.set(meter);

                outputValue.set(
                    "2020=" + consumption2020 +
                    "\t2021=" + consumption2021
                );

                context.write(
                    outputKey,
                    outputValue
                );
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 3) {

            System.err.println(
                "Usage: MapSideJoin " +
                "<2020-summary> " +
                "<2021-summary> " +
                "<output>"
            );

            System.exit(2);
        }

        Configuration conf =
                new Configuration();

        conf.set(
            "join.file",
            args[1]
        );

        Job job =
                Job.getInstance(
                    conf,
                    "Map Side Join 2020 2021"
                );

        job.setJarByClass(
                MapSideJoin.class
        );

        job.setMapperClass(
                JoinMapper.class
        );

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[2])
        );

        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                Text.class
        );

        System.exit(
            job.waitForCompletion(true)
                ? 0 : 1
        );
    }
}
