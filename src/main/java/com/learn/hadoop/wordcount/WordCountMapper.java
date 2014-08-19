package com.learn.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by inheekim on 2014. 8. 12..
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer iter = new StringTokenizer(value.toString());
        while (iter.hasMoreTokens()) {
            word.set(iter.nextToken());
            context.write(word, one);
        }
    }
}
