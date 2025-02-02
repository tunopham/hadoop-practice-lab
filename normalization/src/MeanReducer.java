import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanReducer extends Reducer<Text, Text, Text, Text> {
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
