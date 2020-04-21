package com.chnic.avro;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AvroSerializedFileTest {

    private AvroSerializedFile avroSerializedFile;

    private static Map<Integer, Float> parameterMap;

    @BeforeAll
    public static void beforeAll() {
        parameterMap = new HashMap<>();
        parameterMap.put(2020, (float) 38.1);
        parameterMap.put(2019, (float) 37.6);
        parameterMap.put(2018, (float) 36.9);
    }

    @BeforeEach
    public void setup() {
        avroSerializedFile = new AvroSerializedFile();
    }

    @Test
    @Order(1)
    public void testSerializedFile() throws IOException {
        String filePath = AvroSerializedFileTest.class.getResource("").getPath() + "data.avro";
        avroSerializedFile.serializedFile(filePath, parameterMap);

        assertTrue(new File(filePath).exists());
    }

    @Test
    @Order(2)
    public void testDeserializedFile() throws IOException {
        String filePath = AvroSerializedFileTest.class.getResource("").getPath() + "data.avro";
        List<GenericRecord> recordList = avroSerializedFile.deserializedFile(filePath);

        assertEquals(parameterMap.size(), recordList.size());
    }
}
