import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class main extends Configured implements Tool {

    private static final String NAME_NODE = "hdfs://localhost:9000";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/home/eyedema/hadoop");
        int returnValue = ToolRunner.run(new main(), args);
        System.exit(returnValue);
        // aggiungere molti altri libri (anche 50). Il risultato che si ottiene è ok, ma la rimozione di punteggiatura,
        // numeri e caratteri speciali va fatta dentro Hadoop. Bisogna fare degli altri map/reduce per fare proprio questo
        // prima dell'effettivo calcolo degli ngram. Devo implementare quello degli ngram anche per le parole (basta cambiare
        // poco nel mapper); bisogna vedere se riesco a fare 2 file di output ma non è così importante.
        // si può fare qualche istogramma con i dati in outputh.
        // per quello delle password basta poter vedere un plateau nel grafico. Devo scegliere bene la parola che sto cercando
        // per vedere in che punto dei chunk si trova.
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/home/eyedema/hadoop");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(NAME_NODE), conf);
        fs.delete(new Path("/user/eyedema/books/output/"), true);
        fs.delete(new Path("/user/eyedema/books/output2/"), true);
        Job job = Job.getInstance(conf, "N-gram calculation - letters");
        job.setJarByClass(getClass());
        // MapReduce chaining
        Configuration map1Conf = new Configuration(false);
        ChainMapper.addMapper(job, myMapper.class, Object.class, Text.class,
                Text.class, IntWritable.class, map1Conf);

        Configuration map2Conf = new Configuration(false);
        map2Conf.setInt("n", Integer.parseInt(args[3]));
        ChainMapper.addMapper(job, textInputMapper.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, map2Conf);

        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, myReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, reduceConf);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Job job2 = Job.getInstance(conf, "N-gram calculation - words");

        Configuration map3Conf = new Configuration(false);
        ChainMapper.addMapper(job2, WordMapper.class, Object.class, Text.class,
                Text.class, IntWritable.class, map3Conf);
        Configuration reduceConf2 = new Configuration(false);
        ChainReducer.setReducer(job2, myReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, reduceConf2);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job.waitForCompletion(false);
        job2.waitForCompletion(false);

        return 0;
    }
}