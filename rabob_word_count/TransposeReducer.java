package rabob_word_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TransposeReducer extends Reducer <Text,IntWritable,Text,IntWritable>{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
    }

    @Override
    protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> one = values.iterator();//ble tor
        int sum = 0;
        while(one.hasNext()){
            //IntWritable temp = one.next();
            //sum += temp.get();//
            sum += (one.next()).get();
        }
        context.write(word,new IntWritable(sum));
    }
}
