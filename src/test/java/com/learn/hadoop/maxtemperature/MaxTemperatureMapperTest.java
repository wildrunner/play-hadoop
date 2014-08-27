package com.learn.hadoop.maxtemperature;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.learn.hadoop.wordcount.WordCountMapper;
import com.learn.hadoop.wordcount.WordCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MaxTemperatureMapperTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private List<String> lines;

    @Before
    public void setUp() throws Exception {
        MaxTemperatureMapper mapper = new MaxTemperatureMapper();
        MaxTemperatureReducer reducer = new MaxTemperatureReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        ClassPathResource resource1 = new ClassPathResource("maxtemperature/1901");
        File file1 = resource1.getFile();
        lines = Files.readLines(file1, Charsets.UTF_8);

        ClassPathResource resource2 = new ClassPathResource("maxtemperature/1902");
        File file2 = resource2.getFile();
        lines.addAll(Files.readLines(file2, Charsets.UTF_8));


    }

    @Test
    public void testMap() throws Exception {

    }

    @Test
    public void testMapReduce() throws Exception {
        for (String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }

        mapReduceDriver.withOutput(new Text("1901"), new IntWritable(317));
        mapReduceDriver.withOutput(new Text("1902"), new IntWritable(244));


        mapReduceDriver.runTest();
    }
}