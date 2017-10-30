package rabob_rm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class rm_driver {
    public static void main(String[] args) throws IOException {
        String dst = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst),conf);
        boolean out = fs.delete(new Path(dst),true);
    }
}
