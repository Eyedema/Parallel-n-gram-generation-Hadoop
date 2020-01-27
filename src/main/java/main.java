import letters.CleanerMapperLetters;
import letters.NgramMapperLetters;
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
import words.CleanerMapperWords;
import words.GeneralReducer;
import words.WordMapper;

import java.net.URI;

public class main extends Configured implements Tool {

    private static final String NAME_NODE = "hdfs://localhost:9000";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/home/eyedema/hadoop");
        int returnValue = ToolRunner.run(new main(), args);
        System.exit(returnValue);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", args[6]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(NAME_NODE), conf);
        fs.delete(new Path(args[1]), true);
        fs.delete(new Path(args[2]), true);
        Job job = Job.getInstance(conf, "N-gram calculation - letters");
        job.setJarByClass(getClass());
        // MapReduce chaining
        Configuration map1Conf = new Configuration(false);
        ChainMapper.addMapper(job, CleanerMapperLetters.class, Object.class, Text.class,
                Text.class, IntWritable.class, map1Conf);

        Configuration map2Conf = new Configuration(false);
        map2Conf.setInt("n", Integer.parseInt(args[3]));
        ChainMapper.addMapper(job, NgramMapperLetters.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, map2Conf);
        job.setCombinerClass(GeneralReducer.class);
        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, GeneralReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, reduceConf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Job job2 = Job.getInstance(conf, "N-gram calculation - words");

        Configuration map3Conf = new Configuration(false);
        map3Conf.setBoolean("removeStopWords", Boolean.parseBoolean(args[5]));
        ChainMapper.addMapper(job2, CleanerMapperWords.class, Object.class, Text.class,
                Text.class, IntWritable.class, map3Conf);
        Configuration map4Conf = new Configuration(false);
        map4Conf.setInt("n", Integer.parseInt(args[4]));
        ChainMapper.addMapper(job2, WordMapper.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, map4Conf);
        job2.setCombinerClass(GeneralReducer.class);
        Configuration reduceConf2 = new Configuration(false);
        ChainReducer.setReducer(job2, GeneralReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, reduceConf2);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job.waitForCompletion(false);
        job2.waitForCompletion(false);

        return 0;
    }
}