package com.chnic.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.*;

public class ParquetSerializedFile {

    private final MessageType schema = MessageTypeParser.parseMessageType(
            "message YearTemperature{\n" +
                    "required int32 year;\n" +
                    "required float temperature;\n" +
                    "}"
    );

    public void serializeFile(String filePath, Map<Integer, Float> parameterMap) throws IOException {
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        ParquetWriterBuilder parquetWriterBuilder = new ParquetWriterBuilder(new Path(filePath));

        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(schema, configuration);

        parquetWriterBuilder.withConf(configuration);
        ParquetWriter<Group> parquetWriter = parquetWriterBuilder.build();

        Set<Map.Entry<Integer, Float>> entrySet = parameterMap.entrySet();
        for (Map.Entry<Integer, Float> entry : entrySet) {
            parquetWriter.write(simpleGroupFactory.newGroup()
                    .append("year", entry.getKey())
                    .append("temperature", entry.getValue())
            );
        }
        parquetWriter.close();
    }

    public List<Map.Entry<Integer, Float>> deserializeFile(String filePath) throws IOException {
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        ParquetReader<Group> parquetReader = ParquetReader.builder(groupReadSupport, new Path(filePath)).build();
        List<Map.Entry<Integer, Float>> list = new ArrayList<>();

        Group group;
        while ((group = parquetReader.read()) != null) {
            list.add(new AbstractMap.SimpleImmutableEntry<>(group.getInteger("year", 0), group.getFloat("temperature", 0)));
        }

        return list;
    }

    static class ParquetWriterBuilder extends ParquetWriter.Builder<Group, ParquetWriterBuilder> {

        protected ParquetWriterBuilder(Path path) {
            super(path);
        }

        @Override
        protected ParquetWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Group> getWriteSupport(Configuration conf) {
            return new GroupWriteSupport();
        }
    }
}
