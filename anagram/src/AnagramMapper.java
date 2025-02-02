import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class AnagramMapper extends Mapper<Object, Text, Text, Text> {
    private Text sortedKey = new Text();
    private Text originalLine = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().replaceAll("\\s", ""); // Remove spaces
        char[] chars = line.toCharArray();
        Arrays.sort(chars); // Sort characters
        sortedKey.set(new String(chars)); // Sorted characters as key
        originalLine.set(value); // Original line as value
        context.write(sortedKey, originalLine); // Emit key-value pair
    }
}
