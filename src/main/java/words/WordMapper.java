package words;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class WordMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    private List<List<String>> ngrams(int n, String str) throws IOException {
        StringTokenizer st = new StringTokenizer(str.toString());
        List<List<String>> ngrams = new ArrayList<List<String>>(n);
        for (int i = 0; i < st.countTokens() - n + 1; i++) {
            List<String> l = new ArrayList<String>();
            ngrams.add(l);
        }
        String[] stringa = str.toString().split(" ");
        for (int i = 0; i < st.countTokens() - n + 1; i++) {
            for (int j = i; j < i + n; j++) {
                ngrams.get(i).add(stringa[j]);
            }
        }
        return ngrams;
    }

    @Override
    public void map(Text key, IntWritable value, Mapper.Context context) throws IOException, InterruptedException {
        List<List<String>> ngrams = ngrams(context.getConfiguration().getInt("n", 3), key.toString());
        for (List<String> list : ngrams) {
            Text currentNgram = new Text();
            for (int i = 0; i < list.size(); i++) {
                currentNgram.append(list.get(i).getBytes(), 0, list.get(i).length());
                if (i < list.size()) currentNgram.append(" ".getBytes(), 0, " ".length());
            }
            context.write(currentNgram, one);
        }
    }
}