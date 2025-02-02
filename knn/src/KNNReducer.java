import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KNNReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Output the label and its distance
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
