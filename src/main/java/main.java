import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class main {

    private static final String NAME_NODE = "hdfs://localhost:9000";

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "/home/eyedema/hadoop");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(NAME_NODE), conf);
        fs.delete(new Path("/user/eyedema/books/output/"), true);
        fs.delete(new Path("/user/eyedema/books/output2/"), true);
        Job job = Job.getInstance(conf, "N-gram calculation");
        job.setJarByClass(main.class);
        job.setMapperClass(myMapper.class);
        job.setCombinerClass(myReducer.class);
        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //job.setOutputValueClass(SequenceFileOutputFormat.class);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
        Job job2 = Job.getInstance(conf, "N-gram calculation2");
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setJarByClass(main.class);
        job2.setMapperClass(textInputMapper.class);
        job2.setCombinerClass(myReducer.class);
        job2.setReducerClass(myReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        //if (!job2.waitForCompletion(true)) {
        //    System.exit(1);
        //}
        // aggiungere molti altri libri (anche 50). Il risultato che si ottiene è ok, ma la rimozione di punteggiatura,
        // numeri e caratteri speciali va fatta dentro Hadoop. Bisogna fare degli altri map/reduce per fare proprio questo
        // prima dell'effettivo calcolo degli ngram. Devo implementare quello degli ngram anche per le parole (basta cambiare
        // poco nel mapper); bisogna vedere se riesco a fare 2 file di output ma non è così importante.
        // si può fare qualche istogramma con i dati in outputh.
        // per quello delle password basta poter vedere un plateau nel grafico. Devo scegliere bene la parola che sto cercando
        // per vedere in che punto dei chunk si trova.
    }
}