package rabob_group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Group_reducer extends Reducer<Text,Text,NullWritable,Text>{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> member= values.iterator();
        StringBuilder bag = new StringBuilder();
        bag.append("(").append(key.toString()).append(",").append("{");
        while (member.hasNext()){
            bag.append("(").append(member.next().toString()).append(")").append(",");
        }
        bag.delete(bag.length()-1,bag.length());
        bag.append("})");
        context.write(NullWritable.get(),new Text(bag.toString()));
    }
}
