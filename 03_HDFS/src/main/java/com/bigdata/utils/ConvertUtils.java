package com.bigdata.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.Log;
import parquet.Preconditions;
import parquet.column.page.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class ConvertUtils {

    private static final Log LOG = Log.getLog(ConvertUtils.class);

    public static final String CSV_DELIMITER= ",";

    private static String readFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        StringBuilder stringBuilder = new StringBuilder();

        try {
            String line = null;
            String ls = System.getProperty("line.separator");

            while ((line = reader.readLine()) != null ) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        } finally {
            Utils.closeQuietly(reader);
        }

        return stringBuilder.toString();
    }

    public static String  getSchema(File csvFile) throws IOException {
        String fileName = csvFile.getName().substring(
                0, csvFile.getName().length() - ".csv".length()) + ".schema";
        File schemaFile = new File(csvFile.getParentFile(), fileName);
        return readFile(schemaFile.getAbsolutePath());
    }

    public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
        convertCsvToParquet(csvFile, outputParquetFile, false);
    }

    public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException {
        LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
        String rawSchema = getSchema(csvFile).replace("\r", "").replace("\n","");
        if(outputParquetFile.exists()) {
            throw new IOException("Output file " + outputParquetFile.getAbsolutePath() +
                    " already exists");
        }

        Path path = new Path(outputParquetFile.toURI());

        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        int lineNumber = 0;
        try {
            while ((line = br.readLine()) != null) {
                if (lineNumber++ == 0) continue; // skip the header
                String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
                writer.write(Arrays.asList(fields));
            }
            writer.close();
        } finally {
            LOG.info("Number of lines: " + lineNumber);
            Utils.closeQuietly(br);
        }
    }
}
