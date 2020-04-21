package com.chnic.avro;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AvroGenericRecordTest {

    private AvroGenericRecord record;

    @BeforeEach
    public void setup() {
        record = new AvroGenericRecord();
    }

    @Test
    public void testAvroGenericRecord() throws IOException {
        int year = 2020;
        float temperature = (float) 38.1;

        GenericRecord genericRecord = record.readGenericRecord(record.createGenericRecord(year, temperature));
        assertNotNull(genericRecord);
        assertEquals(year, Integer.parseInt(genericRecord.get("year").toString()));
        assertEquals(temperature, Float.parseFloat(genericRecord.get("temperature").toString()));
        assertEquals("com.chnic.avro.YearTemperature", genericRecord.getSchema().getFullName());
    }
}
