import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
        Job job = Job.getInstance(conf, "N-gram calculation");
        job.setJarByClass(main.class);
        job.setMapperClass(textInputMapper.class);
        job.setCombinerClass(myReducer.class);
        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        // aggiungere molti altri libri (anche 50). Il risultato che si ottiene è ok, ma la rimozione di punteggiatura,
        // numeri e caratteri speciali va fatta dentro Hadoop. Bisogna fare degli altri map/reduce per fare proprio questo
        // prima dell'effettivo calcolo degli ngram. Devo implementare quello degli ngram anche per le parole (basta cambiare
        // poco nel mapper); bisogna vedere se riesco a fare 2 file di output ma non è così importante.
        // si può fare qualche istogramma con i dati in outputh.
        // per quello delle password basta poter vedere un plateau nel grafico. Devo scegliere bene la parola che sto cercando
        // per vedere in che punto dei chunk si trova.
    }
}