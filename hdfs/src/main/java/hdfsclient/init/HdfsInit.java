package hdfsclient.init;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HdfsInit {
    public static FileSystem fileSystem = null;

    public void initHdfs(String ipString) {
        ipString = ipString.trim();
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://" + ipString + ":9000");
        try {
            this.fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void close(FileSystem fileSystem) {
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
