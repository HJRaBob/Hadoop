package rabob_foreach;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        Configuration conf = new Configuration();

        job.getConfiguration().setBoolean("mapreduce.mat.output.compress",true);
        job.getConfiguration().set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", true);
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.codec",
                "org.apache.hadoop.io.compress.GzipCodec");


        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0:1;
    }

    private void parseArguements(String[] args,Job job) throws IOException {
        for (int i = 0; i < args.length; ++i) {
            if ("-input".equals(args[i])) {
                FileInputFormat.addInputPaths(job, args[++i]);
            } else if ("-output".equals(args[i])) {
                FileOutputFormat.setOutputPath(job, new Path(args[++i]));
            } else if ("-column".equals(args[i])) {
                job.getConfiguration().setStrings("column", args[++i]);
            } else if ("-sequence".equals(args[i])){
                job.getConfiguration().setStrings("sequence", args[++i]);
            }
        }
    }

}
