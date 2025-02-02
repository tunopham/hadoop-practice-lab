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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class KNNV2 {

    public static class KNNMapperV2 extends Mapper<Object, Text, Text, DoubleWritable> {
        private Example testExample;
        private static final String TEST_EXAMPLE = "TEST,2.0,3.0";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            testExample = readExample(TEST_EXAMPLE);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Example trainingExample = readExample(value.toString());
            double distance = getDistance(testExample, trainingExample);
            context.write(new Text(trainingExample.getLabel()), new DoubleWritable(distance));
        }

        private Example readExample(String line) {
            String[] tokens = line.split(",");
            String label = tokens[0];
            double[] features = new double[tokens.length - 1];
            for (int i = 1; i < tokens.length; i++) {
                features[i - 1] = Double.parseDouble(tokens[i]);
            }
            return new Example(label, features);
        }

        private double getDistance(Example e1, Example e2) {
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

    public static class KNNReducerV2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final List<Pair> labelDistancePairs = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                labelDistancePairs.add(new Pair(key.toString(), value.get()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(labelDistancePairs, Comparator.comparingDouble(Pair::getDistance));
            for (Pair pair : labelDistancePairs) {
                context.write(new Text(pair.getLabel()), new DoubleWritable(pair.getDistance()));
            }
        }

        private static class Pair {
            private final String label;
            private final double distance;

            public Pair(String label, double distance) {
                this.label = label;
                this.distance = distance;
            }

            public String getLabel() {
                return label;
            }

            public double getDistance() {
                return distance;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: KNN <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "K-Nearest Neighbors");
        job.setJarByClass(KNNV2.class);
        job.setMapperClass(KNNMapperV2.class);
        job.setReducerClass(KNNReducerV2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Deleted existing output directory: " + args[1]);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
