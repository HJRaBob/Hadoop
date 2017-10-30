package rabob_pull;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.InputStream;

public class pull_driver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://70.12.114.244:8020");
        FileSystem fs = FileSystem.get(conf);
        InputStream is = fs.open(new Path("/workspace/movielens/README.txt"));

        FileUtils.copyInputStreamToFile(is, new File("/var/lib/hadoop-hdfs/pull.txt"));
    }
}
