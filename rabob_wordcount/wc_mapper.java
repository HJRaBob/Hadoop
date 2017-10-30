package rabob_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wc_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] columns = row.split(",");
        for (String word : columns) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}