package rabob_wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class wc_driver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new wc_driver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        parseArguements(args, job);

        job.setJarByClass(wc_driver.class);

        // Mapper & Reducer Class
        job.setMapperClass(wc_mapper.class);
        job.setReducerClass(wc_reducer.class);

        // Mapper Output Key & Value Type after Hadoop 0.20
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer Output Key & Value Type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
        job.getConfiguration().set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", true);
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.codec",
                "org.apache.hadoop.io.compress.GzipCodec");

        // Run a Hadoop Job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void parseArguements(String[] args, Job job) throws IOException {
        for (int i = 0; i < args.length; ++i) {
            if ("-input".equals(args[i])) {
                FileInputFormat.addInputPaths(job, args[++i]);
            } else if ("-output".equals(args[i])) {
                FileOutputFormat.setOutputPath(job, new Path(args[++i]));
            }
        }
    }
}