import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class KNNReducerV2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final List<Pair> labelDistancePairs = new ArrayList<>();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        // Collect all distances for the current label
        for (DoubleWritable value : values) {
            labelDistancePairs.add(new Pair(key.toString(), value.get()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort the label-distance pairs by distance
        Collections.sort(labelDistancePairs, Comparator.comparingDouble(Pair::getDistance));

        // Emit the sorted pairs
        for (Pair pair : labelDistancePairs) {
            context.write(new Text(pair.getLabel()), new DoubleWritable(pair.getDistance()));
        }
    }

    // Helper class to store label and distance pairs
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
