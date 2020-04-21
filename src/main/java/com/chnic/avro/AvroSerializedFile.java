package com.chnic.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroSerializedFile {

    public void serializedFile(String filePath, Map<Integer, Float> parameterMap) throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("YearTemperature.avsc"));
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
        dataFileWriter.create(schema, new File(filePath));

        parameterMap.forEach((k, v) -> {
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("year", k);
            genericRecord.put("temperature", v);
            try {
                dataFileWriter.append(genericRecord);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        dataFileWriter.close();
    }

    public List<GenericRecord> deserializedFile(String filePath) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(filePath), reader);
        List<GenericRecord> recordList = new ArrayList<>();
        dataFileReader.forEach(recordList::add);
        dataFileReader.close();
        return recordList;
    }
}
