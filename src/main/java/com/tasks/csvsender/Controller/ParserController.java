package com.tasks.csvsender.Controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Controller;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@PropertySource("classpath:application.properties")
public class ParserController {

    @Value("${parser.extention}")
    String extension;

    @Value("${parser.separator}")
    char separator;

    @Value("${parser.batch.size}")
    int batchSize;

    @Autowired
    RabbitController rabbitController;

    private String[] headersArray;


    public List<File> getFiles(List<String> paths) {
        List<File> allFiles = new ArrayList<File>();
        for (String path : paths) {
            File dir = new File(path);
            File[] files = dir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(extension);
                }
            });
            for (File f : files) {
                allFiles.add(f);
            }
        }
        return allFiles;
    }

    public HashMap<String, List<Object>> mapHeaders (String[] headers){
        HashMap<String, List<Object>> mapTemplate = new HashMap<>();
        for (String header : headers){
            mapTemplate.put(header, new ArrayList<Object>());
        }
        return mapTemplate;
    }

    public String convertToJson(Map<String, List<Object>> map) throws JsonProcessingException {
        StringBuilder stringBuilder = new StringBuilder();
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < map.get(headersArray[0]).size(); i++) {
            HashMap<String, Object> newMap = new HashMap<String, Object>();
            for (String header : headersArray) {
                newMap.put(header, map.get(header).get(i));
            }
            stringBuilder.append(mapper.writeValueAsString(newMap));
        }
        return stringBuilder.toString();
    }

    public void sendFile(File path) throws IOException {
        CSVReaderBuilder readerBuilder = new CSVReaderBuilder(new FileReader(path));
        CSVParser parser = new CSVParserBuilder().withSeparator(separator).build();
        CSVReader reader = readerBuilder.withCSVParser(parser).build();


        headersArray =reader.readNext();

        String[] record;
        int iterator = 1;
        HashMap<String, List<Object>> map = mapHeaders(headersArray);
        while ((record = reader.readNext()) != null){
            for (int i = 0; i<record.length; i++){
                String value = record[i];
                map.get(headersArray[i]).add(value);
            }
            if (iterator%batchSize == 0){
                rabbitController.rabbitSend(convertToJson(map), path.getName());
                map = mapHeaders(headersArray);
            }
            iterator++;
        }
        rabbitController.rabbitSend(convertToJson(map), path.getName());

        reader.close();


        System.out.println("___________________New File___________________");
    }
}
