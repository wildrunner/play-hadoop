package com.learn.hadoop.departure;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by inheekim on 2014. 8. 25..
 */
public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outputKey = new Text();
    private final static IntWritable outputValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] colums = value.toString().split(",");
            if (ArrayUtils.isNotEmpty(colums)) {
                outputKey.set(colums[0] + "," + colums[1]);
                if (!colums[15].equals("NA")) {
                    int depDelayTime = Integer.parseInt(colums[15]);
                    if (depDelayTime > 0) {
                        outputValue.set(depDelayTime);
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }
}
