package com.chnic.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NCDCMaxTemperature extends Configured implements Tool {

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


    static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, FloatWritable> {


        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> outputCollector, Reporter reporter) throws IOException {
            try {
                Map.Entry<Integer, Float> entry = DataParser.parser(value.toString());
                outputCollector.collect(new IntWritable(entry.getKey()), new FloatWritable(entry.getValue()));
            } catch (ValidateException e) {
                reporter.getCounter(ValidateResult.valueOf(e.getMessage())).increment(1);
            }
        }
    }

    static class ReduceClass extends MapReduceBase implements Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

        @Override
        public void reduce(IntWritable key, Iterator<FloatWritable> valueIterator, OutputCollector<IntWritable, FloatWritable> outputCollector, Reporter reporter) throws IOException {
            float maxValue = Float.MIN_VALUE;
            while (valueIterator.hasNext()) {
                maxValue = Math.max(maxValue, valueIterator.next().get());
            }
            outputCollector.collect(key, new FloatWritable(maxValue));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        JobConf jobConf = new JobConf(getConf(), NCDCMaxTemperature.class);
        jobConf.setJobName("Compute Max Temperature");

        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(FloatWritable.class);

        jobConf.setMapperClass(MapClass.class);
        jobConf.setCombinerClass(ReduceClass.class);
        jobConf.setReducerClass(ReduceClass.class);

        FileInputFormat.setInputPaths(jobConf, new Path(strings[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(strings[1]));

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new NCDCMaxTemperature(), args);
        System.exit(result);
    }
}
