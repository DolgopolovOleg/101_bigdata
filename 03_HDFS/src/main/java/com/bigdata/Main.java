package com.bigdata;

import com.bigdata.utils.ConvertUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class Main {

    private static final String HOST = "hdfs://localhost:8020";

    public static void main(String[] args) throws Exception {
        // check input params
        if (args.length != 2) {
            System.out.println("Should be provided 2 parameters: convertAndLocal.jar <local csv file path> <HDFS destination path>");
            System.exit(0);
        }

//        FileSystem fs = new DistributedFileSystem();
//        fs.initialize(new URI(HOST), getConfiguration());

        String fileName = args[0];
        String destination = args[1];

        // create .parquet file
        File csvFile = new File(fileName);
        File prqtFile = new File(fileName.replace(".csv", ".parquet"));
        FileSystem fs = FileSystem.get(new URI(HOST), getConfiguration(), "root");

        try {
            // create a parquet copy of the file in local FS
            ConvertUtils.convertCsvToParquet(csvFile, prqtFile);

            fs.copyFromLocalFile(new Path(csvFile.getAbsolutePath()), new Path(getFullDestinationPath(destination, csvFile.getName())));
            // create file in HDFS
//            InputStream is = new BufferedInputStream(new FileInputStream(csvFile));//Data set is getting copied into input stream through buffer mechanism.
//            FSDataOutputStream os = fs.create(new Path(getFullDestinationPath(destination, csvFile.getName())), true);
//
//            IOUtils.copyBytes(is, os, getConfiguration());
        } finally {
            // remove redundant copy from local FS
            fs.close();
            cleanUp(prqtFile);
        }
    }

    private static void cleanUp(File file) {
        if (file.exists()) file.delete();
        File crcfile = new File(file.getPath().replace(file.getName(), "." + file.getName() + ".crc"));
        if (crcfile.exists()) crcfile.delete();
    }

    private static String getFullDestinationPath(String destination, String fileName){
        return HOST + destination + (destination.endsWith("/") ? "" : "/") + fileName;
    }

    private static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "/user/root/bd_course/");
//        conf.set("dfs.replication", "1");
//        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("dfs.webhdfs.enabled", "true");

        return conf;
    }
}
