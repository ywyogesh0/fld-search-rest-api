package com.fld.search.rest.api.sample;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;

public class CSVCreator {

    public static void main(String[] args) throws IOException {

        try (CSVWriter csvWriter = new CSVWriter(new FileWriter("kafka-sample.csv"))) {
            csvWriter.writeNext(new String[]{"key", "offset", "date"}, false);

            for (int i = 1; i <= 100; i++) {
                csvWriter.writeNext(new String[]{String.valueOf(i), "\t", ""}, false);
            }
        }
    }

}
