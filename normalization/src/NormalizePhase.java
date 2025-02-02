import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class NormalizePhase {

    // Mapper
    public static class NormalizeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private final HashMap<String, Double> means = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Get the local cached files
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                String cacheFileName = new File(cacheFiles[0].getPath()).getName();
                File cacheFile = new File(cacheFileName);

                System.out.println("Cache file detected: " + cacheFile.getAbsolutePath());
                // Read the cache file
                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println("Reading line from cache: " + line); // Debug: print cache file content
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            String feature = parts[0];
                            double mean = Double.parseDouble(parts[1]);
                            means.put(feature, mean);
                            System.out.println("Loaded mean: " + feature + " -> " + mean); // Debug: log loaded mean
                        } else {
                            System.out.println("Invalid line format in cache: " + line); // Debug: invalid format
                        }
                    }
                }
            } else {
                System.out.println("No cache files found."); // Debug: no cache file
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Mapper input: " + value.toString()); // Debug: log mapper input

            String[] parts = value.toString().split(",");
            String label = parts[0];
            StringBuilder normalized = new StringBuilder(label);

            try {
                for (int i = 1; i < parts.length; i++) {
                    double val = Double.parseDouble(parts[i]);
                    String featureKey = "feature" + i;
                    if (means.containsKey(featureKey)) {
                        double mean = means.get(featureKey);
                        double normalizedValue = val - mean;
                        normalized.append(",").append(normalizedValue);
                    } else {
                        System.out.println("Mean not found for key: " + featureKey); // Debug: missing key
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println("Error parsing input value: " + value.toString()); // Debug: parsing error
            }

            String output = normalized.toString();
            System.out.println("Mapper output: " + key + " -> " + output); // Debug: log mapper output
            context.write(key, new Text(output));
        }
    }

    // Reducer
    public static class NormalizeReducer extends Reducer<LongWritable, Text, Text, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(null, value);
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: NormalizeDriver <input path> <output path> <mean file path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(NormalizePhase.class);
        job.setJobName("Normalization");

        // Add the mean file to DistributedCache
        job.addCacheFile(new Path(args[2]).toUri());

        // Set Mapper and Input/Output paths (No Reducer needed)
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class); // Disable Reducer

        job.setMapOutputKeyClass(LongWritable.class);
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
