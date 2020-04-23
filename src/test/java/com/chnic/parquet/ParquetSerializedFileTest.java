package com.chnic.parquet;

import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetSerializedFileTest {

    private ParquetSerializedFile parquetSerializedFile;

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
        parquetSerializedFile = new ParquetSerializedFile();
    }

    @Test
    @Order(1)
    public void testSerializedFile() throws IOException {
        String filePath = ParquetSerializedFileTest.class.getResource("").getPath() + "data.parquet";
        parquetSerializedFile.serializeFile(filePath, parameterMap);

        assertTrue(new File(filePath).exists());
    }

    @Test
    @Order(2)
    public void testDeserializedFile() throws IOException {
        String filePath = ParquetSerializedFileTest.class.getResource("").getPath() + "data.parquet";
        List<Map.Entry<Integer, Float>> list = parquetSerializedFile.deserializeFile(filePath);

        assertNotNull(list);
        assertEquals(parameterMap.size(), list.size());
        list.forEach(v -> assertEquals(parameterMap.get(v.getKey()), v.getValue()));
    }
}
