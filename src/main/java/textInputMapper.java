import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class textInputMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    private List<String> ngrams(int n, String str) {
        List<String> ngrams = new ArrayList<String>();
        for (int i = 0; i < str.length() - n + 1; i++) {
            String temp = str.substring(i, i + n);
            ngrams.add(temp);
        }
        return ngrams;
    }

    @Override
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        List<String> wordNgrams = ngrams(3, key.toString());
        for (String ngram : wordNgrams) {
            Text tmpWord = new Text(ngram);
            context.write(tmpWord, value);
        }
    }
}
