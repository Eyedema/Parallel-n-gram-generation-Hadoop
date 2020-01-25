package words;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CleanerMapperWords extends Mapper<Object, Text, Text, IntWritable> {

    IntWritable one = new IntWritable(1);

    public List<String> loadStopwords() throws IOException {
        List<String> stopwords = Files.readAllLines(Paths.get("src/main/java/stopwords.txt"));
        return stopwords;
    }

    public Text removeStopWords(Text text) throws IOException {
        List<String> stopwords = loadStopwords();
        ArrayList<String> allWords =
                Stream.of(text.toString().split(" "))
                        .collect(Collectors.toCollection(ArrayList<String>::new));
        Text toRet = new Text();
        allWords.removeAll(stopwords);
        for (String word : allWords) {
            toRet.append(word.getBytes(), 0, word.length());
            toRet.append(" ".getBytes(), 0, " ".length());
        }
        return toRet;
    }

    private Text cleanText(String text, Context context) throws IOException {
        StringTokenizer st = new StringTokenizer(text, " '!()-[]{};:,<>./?@#$%^&*_~—”“\n\t\r");
        Text textRet = new Text();
        while (st.hasMoreTokens()) {
            String token = st.nextToken().toLowerCase().replaceAll("[0-9]", "");
            if (token.matches("\\w+")) {
                textRet.append(token.getBytes(), 0, token.length());
                textRet.append(" ".getBytes(), 0, " ".length());
            }
        }
        if (context.getConfiguration().getBoolean("removeStopWords", true)) {
            textRet = removeStopWords(textRet);
        }
        return textRet;
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        Text newValue = cleanText(value.toString(), context);
        context.write(newValue, one);
    }
}
