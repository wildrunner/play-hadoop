package com.learn.hadoop.wordcount;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
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
import java.util.ArrayList;
import java.util.List;

public class WordCountMapperTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private String testInputText;

    @Before
    public void setUp() throws Exception {
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        ClassPathResource resource = new ClassPathResource("input.txt");
        File file = resource.getFile();
        String testInputText = Files.toString(file, Charsets.UTF_8);
        this.testInputText = testInputText.replaceAll(System.getProperty("line.separator"), " ");
    }

    @Test
    public void testMap() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text(testInputText));

        mapDriver.withOutput(new Text("read"), new IntWritable(1));
        mapDriver.withOutput(new Text("a"), new IntWritable(1));
        mapDriver.withOutput(new Text("book"), new IntWritable(1));
        mapDriver.withOutput(new Text("write"), new IntWritable(1));
        mapDriver.withOutput(new Text("a"), new IntWritable(1));
        mapDriver.withOutput(new Text("book"), new IntWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new Text("book"), values);
        reduceDriver.withOutput(new Text("book"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(), new Text(testInputText));

        mapReduceDriver.withOutput(new Text("a"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("book"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("read"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("write"), new IntWritable(1));

        mapReduceDriver.runTest();
    }
}