import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class main {

    private static final String NAME_NODE = "hdfs://localhost:9000";

    public static void main(String[] args) throws URISyntaxException, IOException {
        System.setProperty("hadoop.home.dir", "/home/eyedema/hadoop");
        HDFSFileReader hdfsfr = new HDFSFileReader(NAME_NODE, args[0]);
        ArrayList<String> books = hdfsfr.readBooks();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "N-gram calculation");
        job.setJarByClass(main.class);
        job.setMapperClass(myMapper.class);
        job.setCombinerClass(myReducer.class);
        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }

}