import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNDriverV2 {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: KNNDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "K-Nearest Neighbors");
        job.setJarByClass(KNNDriverV2.class);

        job.setMapperClass(KNNMapperV2.class);
        job.setReducerClass(KNNReducerV2.class);

        // Set mapper output types
        job.setMapOutputKeyClass(Text.class); // Mapper key: distance
        job.setMapOutputValueClass(DoubleWritable.class);        // Mapper value: label

        // Set reducer output types
        job.setOutputKeyClass(Text.class);             // Reducer key: label
        job.setOutputValueClass(DoubleWritable.class); // Reducer value: distance

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
