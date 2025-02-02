import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class Anagram {

    // Mapper
    public static class AnagramMapper extends Mapper<Object, Text, Text, Text> {
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

    // Reducer
    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {
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

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Anagram <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram Finder");

        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
