package com.chnic.avro;

import org.apache.hadoop.shaded.org.apache.avro.Schema;
import org.apache.hadoop.shaded.org.apache.avro.generic.GenericData;
import org.apache.hadoop.shaded.org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.shaded.org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.shaded.org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.shaded.org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroGenericRecord {


    public byte[] createGenerateRecord() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("YearTemperature.avsc"));

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("year", 2020);
        genericRecord.put("temperature", (float) 38.1);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        writer.write(genericRecord, encoder);
        encoder.flush();
        byteArrayOutputStream.close();

        return byteArrayOutputStream.toByteArray();
    }

    public void readGenericRecord(byte[] bytes) throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("YearTemperature.avsc"));
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord genericRecord = reader.read(null, decoder);

        System.out.println(genericRecord.getSchema().toString());
        System.out.println(genericRecord.get("year"));
        System.out.println(genericRecord.get("temperature"));
    }

    public static void main(String[] args) throws IOException {
        AvroGenericRecord record = new AvroGenericRecord();
        record.readGenericRecord(record.createGenerateRecord());
    }
}
