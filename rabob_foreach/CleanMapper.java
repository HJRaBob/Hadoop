package rabob_foreach;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CleanMapper extends Mapper <LongWritable, Text, NullWritable,Text>{
    int column;
    int sequence;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        column = context.getConfiguration().getInt("column", 0);
        sequence = context.getConfiguration().getInt("sequence", 0);
    }

    @Override
    protected void map(LongWritable line, Text count, Context context) throws IOException, InterruptedException {
        String string_line = count.toString();
        StringBuilder link = new StringBuilder();
        String[] word = string_line.split(",");
        if(word.length == column){
            for(int i = 0;i<sequence;i++){
                link.append(word[i]).append(",");
            }
            for(int i = sequence+1;i<column;i++){
                link.append(word[i]).append(",");
            }
        }
        int link_length = link.toString().length();
        context.write(NullWritable.get(),new Text(link.toString().substring(0,link_length-1)));
    }
}
