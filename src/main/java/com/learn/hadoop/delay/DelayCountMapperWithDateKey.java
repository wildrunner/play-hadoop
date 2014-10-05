package com.learn.hadoop.delay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by inheekim on 2014. 9. 22..
 */
public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {

    private String workType;

    private DateKey outputKey = new DateKey();
    private final static IntWritable outputValue = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        workType = context.getConfiguration().get("workType");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",");
            if (columns != null && columns.length > 0) {
                if (!columns[15].equals("NA")) {
                    int depDelayTime = Integer.parseInt(columns[15]);
                    if (depDelayTime > 0) {
                        outputKey.setYear("D," + columns[0]);
                        outputKey.setMonth((new Integer(columns[1])));
                        context.write(outputKey, outputValue);
                    }
                }
                if (!columns[14].equals("NA")) {
                    int arrDelayTime = Integer.parseInt(columns[14]);
                    if (arrDelayTime > 0) {
                        outputKey.setYear("A," + columns[0]);
                        outputKey.setMonth((new Integer(columns[1])));
                        context.write(outputKey, outputValue);
                    }
                }
            }

        }
    }
}
