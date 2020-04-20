package com.chnic.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NCDCMaxTemperatureSortedByMapper extends Configured implements Tool {

    enum ValidateResult {
        MISSING,
        MALFORMED
    }

    static class ValidateException extends Exception {
        public ValidateException(String s) {
            super(s);
        }
    }

    static class DataParser {
        private static final Pattern pattern = Pattern.compile("^.{15}(\\d{4}).{68}([+-]\\d{4})(\\d).*$");

        private static final int MISSING = 9999;

        public static Map.Entry<Integer, Float> parser(String line) throws ValidateException {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.matches()) {
                throw new ValidateException(ValidateResult.MALFORMED.name());
            }

            int year = Integer.parseInt(matcher.group(1));
            int temperature = Integer.parseInt(matcher.group(2));
            String qualityCode = matcher.group(3);

            if (temperature == MISSING || qualityCode.matches("[^01459]")) {
                throw new ValidateException(ValidateResult.MISSING.name());
            }

            return new AbstractMap.SimpleEntry<>(year, (float) (temperature / 10.0));
        }
    }

    static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, YearTemperatureWritable, NullWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<YearTemperatureWritable, NullWritable> outputCollector, Reporter reporter) throws IOException {
            try {
                Map.Entry<Integer, Float> entry = DataParser.parser(value.toString());
                outputCollector.collect(new YearTemperatureWritable(entry.getKey(), entry.getValue()), NullWritable.get());
            } catch (ValidateException e) {
                reporter.getCounter(ValidateResult.valueOf(e.getMessage())).increment(1);
            }
        }
    }

    static class ReduceClass extends MapReduceBase implements Reducer<YearTemperatureWritable, NullWritable, IntWritable, FloatWritable> {

        @Override
        public void reduce(YearTemperatureWritable key, Iterator<NullWritable> valueIterator, OutputCollector<IntWritable, FloatWritable> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(new IntWritable(key.getYear()), new FloatWritable(key.getTemperature()));
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

            return Float.compare(((YearTemperatureWritable) a).getTemperature(), ((YearTemperatureWritable) b).getTemperature());
        }
    }

    static class YearPartitioner implements Partitioner<YearTemperatureWritable, NullWritable> {

        @Override
        public void configure(JobConf jobConf) {

        }

        @Override
        public int getPartition(YearTemperatureWritable yearTemperatureWritable, NullWritable nullWritable, int partitionerNumbers) {
            return Math.abs(yearTemperatureWritable.getYear() * 53) % partitionerNumbers;
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        JobConf jobConf = new JobConf(getConf(), NCDCMaxTemperatureSortedByMapper.class);
        jobConf.setJobName("Compute Max Temperature");

        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(FloatWritable.class);

//        jobConf.setMapperClass(MapClass.class);
//        jobConf.setGroupingComparatorClass(null);

        jobConf.setReducerClass(ReduceClass.class);

        return 0;
    }
}
