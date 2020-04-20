package com.chnic.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

public class NCDCMaxTemperatureSortedByMapper extends Configured implements Tool {

    static class MapClass extends Mapper<LongWritable, Text, YearTemperatureWritable, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Map.Entry<Integer, Float> entry = RowDataParser.parser(value.toString());
                context.write(new YearTemperatureWritable(entry.getKey(), entry.getValue()), NullWritable.get());
            } catch (ValidateException e) {
                context.getCounter(ValidateResult.valueOf(e.getMessage())).increment(1);
            }
        }
    }

    static class ReduceClass extends Reducer<YearTemperatureWritable, NullWritable, IntWritable, FloatWritable> {

        @Override
        protected void reduce(YearTemperatureWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(key.getYear()), new FloatWritable(key.getTemperature()));
        }
    }

    static class YearComparator extends WritableComparator {

        public YearComparator() {
            super(YearTemperatureWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return Integer.compare(((YearTemperatureWritable) a).getYear(), ((YearTemperatureWritable) b).getYear());
        }
    }

    static class YearTemperatureComparator extends WritableComparator {
        public YearTemperatureComparator() {
            super(YearTemperatureWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            int result = Integer.compare(((YearTemperatureWritable) a).getYear(), ((YearTemperatureWritable) b).getYear());
            if (result != 0) {
                return result;
            }

            return Float.compare(((YearTemperatureWritable) a).getTemperature(), ((YearTemperatureWritable) b).getTemperature()) * -1;
        }
    }

    static class YearPartitioner extends Partitioner<YearTemperatureWritable, NullWritable> {

        @Override
        public int getPartition(YearTemperatureWritable yearTemperatureWritable, NullWritable nullWritable, int i) {
            return Math.abs(yearTemperatureWritable.getYear() * 53) % i;
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "Compute Max Temperature by Sorted Mapper");
        job.setJarByClass(NCDCMaxTemperatureSortedByMapper.class);

        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(YearTemperatureWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(YearComparator.class);
        job.setSortComparatorClass(YearTemperatureComparator.class);
        job.setPartitionerClass(YearPartitioner.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setReducerClass(ReduceClass.class);

        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new NCDCMaxTemperatureSortedByMapper(), args);
        System.exit(result);
    }
}
