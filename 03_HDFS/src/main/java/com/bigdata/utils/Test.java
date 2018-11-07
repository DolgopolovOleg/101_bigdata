package com.bigdata.utils;

import java.io.File;
import java.io.IOException;

/**
 * To convert .csv file to .parquet one .schema file should be provided.
 * Schema is necessary for conversion.
 * Schema should be placed at the same directory with .csv file and has the same name (with .schema extension).
 */
public class Test {

    public static void main(String[] args) throws IOException {
        String fileName = args[0];

        // fill the directory
        init(fileName);

        File csvFile = new File(fileName);
        File prqtFile = new File(fileName.replace(".csv", ".parquet"));

        ConvertUtils.convertCsvToParquet(csvFile, prqtFile);
        System.out.println("DONE!");
    }

    private static void init(String fileName) {
        File f2 = new File(fileName.replace(".csv",".parquet"));
        if (f2.exists()) f2.delete();
    }
}
