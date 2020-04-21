package com.chnic.avro;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AvroGeneratedRecordTest {

    private AvroGeneratedRecord record;

    @BeforeEach
    public void setup() {
        record = new AvroGeneratedRecord();
    }

    @Test
    public void testAvroGeneratedRecord() throws IOException {
        int year = 2021;
        float temperature = (float) 37.8;

        YearTemperature yearTemperature = record.readGeneratedRecord(record.createGeneratedRecord(year, temperature));
        assertNotNull(yearTemperature);
        assertEquals(year, yearTemperature.getYear());
        assertEquals(temperature, yearTemperature.getTemperature());
    }

}
