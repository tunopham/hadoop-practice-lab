import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MeanPhase {

    // Mapper
    public static class MeanMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            for (int i = 1; i < parts.length; i++) {
                context.write(new Text("feature" + i), new Text(parts[i]));
            }
        }
    }

    // Reducer
    public static class MeanReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0.0;

            for (Text value : values) {
                double val = Double.parseDouble(value.toString());
                sum += val;
                count++;
            }

            double mean = sum / count;
            context.write(key, new Text(String.valueOf(mean)));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        // Configure the Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate Means");

        // Set the main classes
        job.setJarByClass(MeanPhase.class);
        job.setMapperClass(MeanMapper.class);
        job.setReducerClass(MeanReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input file path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output file path

        // Run the job and wait for it to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
