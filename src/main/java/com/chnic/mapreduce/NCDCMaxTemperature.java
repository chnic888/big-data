package com.chnic.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

public class NCDCMaxTemperature extends Configured implements Tool {

    static class MapClass extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Map.Entry<Integer, Float> entry = RowDataParser.parser(value.toString());
                context.write(new IntWritable(entry.getKey()), new FloatWritable(entry.getValue()));
            } catch (ValidateException e) {
                context.getCounter(ValidateResult.valueOf(e.getMessage())).increment(1);
            }
        }
    }

    static class ReduceClass extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float maxValue = Float.MIN_VALUE;

            for (FloatWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(key, new FloatWritable(maxValue));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "Compute Max Temperature");
        job.setJarByClass(NCDCMaxTemperature.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);

        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new NCDCMaxTemperature(), args);
        System.exit(result);
    }
}
