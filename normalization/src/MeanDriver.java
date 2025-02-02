import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanDriver {
    // Driver
    public static void main(String[] args) throws Exception {
        // Configuration du job Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calcul des moyennes");

        // Définir les classes principales
        job.setJarByClass(MeanDriver.class);
        job.setMapperClass(MeanMapper.class);
        job.setReducerClass(MeanReducer.class);

        // Définir les types des clés et valeurs de sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0])); // Chemin du fichier d'entrée
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Chemin pour stocker les résultats

        // Lancer le job et attendre qu'il se termine
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
