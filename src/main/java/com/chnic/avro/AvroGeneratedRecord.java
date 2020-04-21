package com.chnic.avro;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroGeneratedRecord {

    public byte[] createGeneratedRecord(int year, float temperature) throws IOException {
        YearTemperature yearTemperature = new YearTemperature();
        yearTemperature.setYear(year);
        yearTemperature.setTemperature(temperature);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DatumWriter<YearTemperature> writer = new SpecificDatumWriter<>(YearTemperature.class);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        writer.write(yearTemperature, encoder);
        encoder.flush();
        byteArrayOutputStream.close();

        return byteArrayOutputStream.toByteArray();
    }

    public YearTemperature readGeneratedRecord(byte[] bytes) throws IOException {
        DatumReader<YearTemperature> reader = new SpecificDatumReader<>(YearTemperature.class);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }
}
