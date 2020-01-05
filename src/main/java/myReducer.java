import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable sum = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int valuesSum = 0;
        for (IntWritable value : values) {
            valuesSum += value.get();
        }
        sum.set(valuesSum);
        context.write(key, sum);
    }
}