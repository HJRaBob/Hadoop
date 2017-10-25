package rabob_word_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CleanMapper extends Mapper <LongWritable, Text, Text,IntWritable>{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
    }

    @Override
    protected void map(LongWritable line, Text count, Context context) throws IOException, InterruptedException {
        String string_line = count.toString();
        String[] word = string_line.split(" ");
        for(int i = 0;i< word.length;i++) {
            context.write(new Text(word[i]), new IntWritable(1));//
        }
    }
}
