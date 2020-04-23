package com.chnic.parquet;

import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AvroParquetSerializedFileTest {

    private AvroParquetSerializedFile avroParquetSerializedFile;

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
        avroParquetSerializedFile = new AvroParquetSerializedFile();
    }

    @Test
    @Order(1)
    public void testSerializedFile() throws IOException {
        String filePath = AvroParquetSerializedFileTest.class.getResource("").getPath() + "avro_data.parquet";
        avroParquetSerializedFile.serializeFile(filePath, parameterMap);

        assertTrue(new File(filePath).exists());
    }

    @Test
    @Order(2)
    public void testDeserializedFile() throws IOException {
        String filePath = AvroParquetSerializedFileTest.class.getResource("").getPath() + "avro_data.parquet";
        List<Map.Entry<Integer, Float>> list = avroParquetSerializedFile.deserializeFile(filePath);

        assertNotNull(list);
        assertEquals(parameterMap.size(), list.size());
        list.forEach(v -> assertEquals(parameterMap.get(v.getKey()), v.getValue()));
    }
}
