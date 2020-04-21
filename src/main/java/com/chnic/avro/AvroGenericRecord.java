package com.chnic.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroGenericRecord {

    public byte[] createGenericRecord(int year, float temperature) throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("YearTemperature.avsc"));

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("year", year);
        genericRecord.put("temperature", temperature);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        writer.write(genericRecord, encoder);
        encoder.flush();
        byteArrayOutputStream.close();

        return byteArrayOutputStream.toByteArray();
    }

    public GenericRecord readGenericRecord(byte[] bytes) throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("YearTemperature.avsc"));
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }
}
