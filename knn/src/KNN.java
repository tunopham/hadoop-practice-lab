import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class KNN {
    // Mapper
    public static class KNNMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        private Example testExample;
        private static final String TEST_EXAMPLE = "TEST,2.0,3.0";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Initialize the test example used for distance calculation
            testExample = readExample(TEST_EXAMPLE);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the current training example
            Example trainingExample = readExample(value.toString());

            // Calculate the distance between the test example and the training example
            double distance = getDistance(testExample, trainingExample);

            // Emit the distance as the key and the label as the value for sorting by distance
            context.write(new DoubleWritable(distance), new Text(trainingExample.getLabel()));
        }

        private Example readExample(String line) {
            // Split the line by commas
            String[] tokens = line.split(",");
            String label = tokens[0]; // First token is the label
            double[] features = new double[tokens.length - 1];

            // Parse the features
            for (int i = 1; i < tokens.length; i++) {
                features[i - 1] = Double.parseDouble(tokens[i]);
            }

            return new Example(label, features);
        }

        private double getDistance(Example e1, Example e2) {
            // Compute Euclidean distance between two examples
            double sum = 0.0;
            for (int i = 0; i < e1.getFeatures().length; i++) {
                sum += Math.pow(e1.getFeatures()[i] - e2.getFeatures()[i], 2);
            }
            return Math.sqrt(sum);
        }

        static class Example {
            private final String label;
            private final double[] features;

            public Example(String label, double[] features) {
                this.label = label;
                this.features = features;
            }

            public String getLabel() {
                return label;
            }

            public double[] getFeatures() {
                return features;
            }
        }
    }

    // Reducer
    public static class KNNReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Output the label and its distance
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: KNNDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "K-Nearest Neighbors");
        job.setJarByClass(KNN.class);
        job.setMapperClass(KNNMapper.class);
        job.setReducerClass(KNNReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set input and output paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Automatically delete the output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // 'true' for recursive deletion
            System.out.println("Deleted existing output directory: " + args[1]);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
