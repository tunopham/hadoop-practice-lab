import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnagramReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder anagrams = new StringBuilder();
        for (Text val : values) {
            if (anagrams.length() > 0) {
                anagrams.append(", ");
            }
            anagrams.append(val.toString());
        }
        result.set(anagrams.toString());
        context.write(key, result); // Emit key and concatenated anagrams
    }
}
