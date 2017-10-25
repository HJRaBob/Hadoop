package rabob_group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Group_mapper extends Mapper<LongWritable,Text,Text,Text>{
    int group_num;
    int column;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        group_num = context.getConfiguration().getInt("group",0);
        column = context.getConfiguration().getInt("column",0);
    }

    @Override
    protected void map(LongWritable line, Text data, Context context) throws IOException, InterruptedException {
        String string = data.toString();
        String[] one_line = string.split(",");
        if(column == one_line.length){
            context.write(new Text(one_line[group_num]),data);
        }


    }
}
