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
        GenericRecord genericRecord = record.readGenericRecord(record.createGenerateRecord());
        assertNotNull(genericRecord);
        assertEquals(2020, Integer.parseInt(genericRecord.get("year").toString()));
        assertEquals(new Float(38.1), Float.parseFloat(genericRecord.get("temperature").toString()));
        assertEquals("com.chnic.avro.YearTemperature", genericRecord.getSchema().getFullName());
    }
}
