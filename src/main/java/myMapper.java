import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class myMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text ngramWord = new Text();
    private Text currentWord = new Text();

    public static List<String> ngrams(int n, String str) {
        List<String> ngrams = new ArrayList<String>();
        for (int i = 0; i < str.length() - n + 1; i++) {
            ngrams.add(str.substring(i, i + n));
        }
        return ngrams;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            currentWord.set(itr.nextToken());
            List<String> currentWordNGrams = ngrams(3, currentWord.toString());
            for (String ngram : currentWordNGrams) {
                ngramWord.set(ngram);
                context.write(ngramWord, one);
            }
        }
    }
}
