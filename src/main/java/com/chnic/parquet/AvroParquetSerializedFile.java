package com.chnic.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.*;

public class AvroParquetSerializedFile {

    public void serializeFile(String filePath, Map<Integer, Float> parameterMap) throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("YearTemperature.avsc"));
        AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(new Path(filePath));

        Configuration configuration = new Configuration();
        AvroWriteSupport.setSchema(configuration, schema);
        ParquetWriter<GenericRecord> parquetWriter = builder.withConf(configuration).withSchema(schema).build();

        Set<Map.Entry<Integer, Float>> entrySet = parameterMap.entrySet();
        for (Map.Entry<Integer, Float> entry : entrySet) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("year", entry.getKey());
            record.put("temperature", entry.getValue());
            parquetWriter.write(record);
        }

        parquetWriter.close();
    }

    public List<Map.Entry<Integer, Float>> deserializeFile(String filePath) throws IOException {
        ParquetReader.Builder<GenericRecord> builder = AvroParquetReader.builder(new AvroReadSupport<>(), new Path(filePath));
        ParquetReader<GenericRecord> parquetReader = builder.build();

        GenericRecord record;
        List<Map.Entry<Integer, Float>> list = new ArrayList<>();

        while ((record = parquetReader.read()) != null) {
            list.add(new AbstractMap.SimpleImmutableEntry<>(Integer.parseInt(record.get("year").toString()), Float.parseFloat(record.get("temperature").toString())));
        }

        return list;
    }
}
