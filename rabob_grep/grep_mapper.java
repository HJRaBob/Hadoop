package rabob_grep;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class grep_mapper extends Mapper<LongWritable,Text,NullWritable,Text> {
    String serch;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        serch = context.getConfiguration().get("serch");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Pattern pattern = Pattern.compile(".*"+serch+".*");
        Matcher matcher;

        for(int i = 0;i<line.length;i++){
            matcher = pattern.matcher(line[i]);
            if (matcher.matches()){
                context.write(NullWritable.get(),value);
            }
        }
    }
}
