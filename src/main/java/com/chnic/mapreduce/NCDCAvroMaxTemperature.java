package com.chnic.mapreduce;

import com.chnic.avro.YearTemperature;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

public class NCDCAvroMaxTemperature extends Configured implements Tool {

    static class MapClass extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<YearTemperature>> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Map.Entry<Integer, Float> entry = RowDataParser.parser(value.toString());

                YearTemperature record = new YearTemperature();
                record.setYear(entry.getKey());
                record.setTemperature(entry.getValue());

                context.write(new AvroKey<>(entry.getKey()), new AvroValue<>(record));
            } catch (ValidateException e) {
                context.getCounter(ValidateResult.valueOf(e.getMessage())).increment(1);
            }
        }
    }

    static class ReduceClass extends Reducer<AvroKey<Integer>, AvroValue<YearTemperature>, AvroKey<YearTemperature>, NullWritable> {
        
        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<YearTemperature>> values, Context context) throws IOException, InterruptedException {
            float maxValue = Float.MIN_VALUE;
            YearTemperature finalRecord = null;

            for (AvroValue<YearTemperature> value : values) {
                YearTemperature genericRecord = value.datum();
                if (genericRecord.getTemperature() > maxValue) {
                    maxValue = genericRecord.getTemperature();
                    finalRecord = genericRecord;
                }
            }

            context.write(new AvroKey<>(finalRecord), NullWritable.get());
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "Compute Max Temperature by Avro");
        job.setMapperClass(MapClass.class);
        job.setJarByClass(NCDCAvroMaxTemperature.class);

        job.setReducerClass(ReduceClass.class);

        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, YearTemperature.getClassSchema());

        AvroJob.setOutputKeySchema(job, YearTemperature.getClassSchema());
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.NULL));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new NCDCAvroMaxTemperature(), args);
        System.exit(result);
    }
}
