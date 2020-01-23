import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    private List<List<String>> ngrams(int n, String str) {
        StringTokenizer st = new StringTokenizer(cleanText(str).toString());

        List<List<String>> ngrams = new ArrayList<List<String>>(n);
        for (int i = 0; i < st.countTokens() - n + 1; i++) {
            List<String> l = new ArrayList<String>();
            ngrams.add(l);
        }
        String[] stringa = cleanText(str).toString().split(" ");
        for (int i = 0; i < st.countTokens() - n + 1; i++) {
            for (int j = i; j < i + n; j++) {
                ngrams.get(i).add(stringa[j]);
            }
        }
        return ngrams;
    }

    private Text cleanText(String text) {
        StringTokenizer st = new StringTokenizer(text, " '!()-[]{};:,<>./?@#$%^&*_~—”“\n");
        Text textRet = new Text();
        while (st.hasMoreTokens()) {
            String token = st.nextToken().toLowerCase().replaceAll("[0-9]", "");
            textRet.append(token.getBytes(), 0, token.length());
            textRet.append(" ".getBytes(), 0, " ".length());
        }
        return textRet;
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        List<List<String>> ngrams = ngrams(3, value.toString());
        for (List<String> list : ngrams) {
            Text currentNgram = new Text();
            for (int i = 0; i < list.size(); i++) {
                currentNgram.append(list.get(i).getBytes(), 0, list.get(i).length());
                if (i < list.size()) currentNgram.append(" ".getBytes(), 0, " ".length());
            }
            System.out.println(currentNgram.toString());
            context.write(currentNgram, one);
        }
    }
}