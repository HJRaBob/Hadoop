package rabob_replace;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class replace_mapper extends Mapper<LongWritable,Text,NullWritable,Text> {
    String r_val,o_val;
    int total,column;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        r_val = context.getConfiguration().get("r_val").trim();
        o_val = context.getConfiguration().get("o_val").trim();
        total = context.getConfiguration().getInt("total",4);
        column = context.getConfiguration().getInt("column",0);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        StringBuilder new_line = new StringBuilder();
        if (total == line.length){
            if (line[column].equals(o_val)){
                line[column] = r_val;
            }

            for(int i = 0;i<line.length;i++){
                new_line.append(line[i]).append(",");
            }
            new_line.delete(new_line.length()-1,new_line.length());
        }
        context.write(NullWritable.get(),new Text(new_line.toString()));
    }
}
