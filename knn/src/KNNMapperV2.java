import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KNNMapperV2 extends Mapper<Object, Text, Text, DoubleWritable> {

    private Example testExample;
    private static final String TEST_EXAMPLE = "TEST,2.0,3.0";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Parse the test example
        testExample = readExample(TEST_EXAMPLE);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the current training example
        Example trainingExample = readExample(value.toString());

        // Calculate the distance between the test example and the training example
        double distance = getDistance(testExample, trainingExample);

        // Emit the distance as the key and the label as the value for sorting by distance
        context.write(new Text(trainingExample.getLabel()), new DoubleWritable(distance));
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
