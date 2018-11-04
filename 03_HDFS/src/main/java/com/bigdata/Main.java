package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class Main {

    private static final String HOST = "hdfs://localhost:8020/";

    public static void main(String[] args) throws Exception {
        // fetch all the
        if (args.length == 0) {
            System.out.println("Should be provided at list one file");
        }

        FileSystem fs = new DistributedFileSystem();
        fs.initialize(new URI(HOST), getConfiguration());

        for (String p : args) {
            File csvFile = getFile(p, fs);
            String rawSchema = getSchema(csvFile);

        }
    }

    private static Configuration getConfiguration () {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "/user/root/bd_course/");
        return conf;
    }

    private static File getFile(String path, FileSystem fs) throws IOException {
        FSDataInputStream fileStream = fs.open(new Path(path));

        //convert FSDataInputStream to File
        File file = new File("");



        fs.close();

        return file;
    }
}
