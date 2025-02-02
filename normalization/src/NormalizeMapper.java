import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;

public class NormalizeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private final HashMap<String, Double> means = new HashMap<>();

    // Helper method to list all files in a directory and its subdirectories
    private void listAllFiles(File dir) {
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory()) {
                listAllFiles(file); // Recursively list files in subdirectories
            } else {
                System.out.println("File found: " + file.getAbsolutePath());
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException {
        // Log the current working directory
        File workingDir = new File(".");
        System.out.println("Working directory: " + workingDir.getAbsolutePath());
        System.out.println("Listing all files in the working directory and subdirectories:");
        listAllFiles(workingDir);

        // Get the cache files added by the Driver
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            System.out.println("Cache file detected: " + cacheFiles[0].getPath());
            // Read the first (and only) cache file
            try (BufferedReader reader = new BufferedReader(new FileReader("./part-r-00000"))) {
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
