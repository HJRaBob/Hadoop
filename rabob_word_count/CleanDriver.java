package rabob_word_count;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CleanDriver extends org.apache.hadoop.conf.Configured
                            implements  org.apache.hadoop.util.Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CleanDriver(),args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        parseArguements(strings,job);
        job.setJarByClass(CleanDriver.class);//java class가 로딩되게 함
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(TransposeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(5);
        return job.waitForCompletion(true) ? 0:1;
    }

    private void parseArguements(String[] args,Job job) throws IOException {
        for (int i = 0; i < args.length; ++i) {
            if ("-input".equals(args[i])) {
                FileInputFormat.addInputPaths(job, args[++i]);
            } else if ("-output".equals(args[i])) {
                FileOutputFormat.setOutputPath(job, new Path(args[++i]));
            }
        }
    }

}
