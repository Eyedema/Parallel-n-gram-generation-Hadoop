import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class textInputMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text parsedBook = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), " '!()-[]{};:,<>./?@#$%^&*_~—”“\n");
        while (itr.hasMoreTokens()) {
            String tempWord = itr.nextToken().toLowerCase().replaceAll("[0-9]", "");
            if (tempWord.matches("\\w+")) {
                tempWord = tempWord + " ";
                parsedBook.append(tempWord.getBytes(), 0, tempWord.length());
            }
        }
        context.write(parsedBook, one);
    }
}
