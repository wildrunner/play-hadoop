package com.learn.hadoop.delay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by inheekim on 2014. 8. 31..
 */
public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {

    private MultipleOutputs<DateKey, IntWritable> mos;

    private DateKey outputKey = new DateKey();
    private IntWritable result = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<DateKey, IntWritable>(context);
    }

    @Override
    protected void reduce(DateKey key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String[] colums = key.getYear().split(",");

        int sum = 0;
        Integer bMonth = key.getMonth();

        if (colums[0].equals("D")) {
            for (IntWritable value : values) {
                if (bMonth != key.getMonth()) {
                    result.set(sum);
                    outputKey.setYear(key.getYear().substring(2));
                    outputKey.setMonth(bMonth);
                    mos.write("departure", outputKey, result);
                    sum = 0;
                }
                sum += value.get();
                bMonth = key.getMonth();
            }
            if (key.getMonth() == bMonth) {
                result.set(sum);
                outputKey.setYear(key.getYear().substring(2));
                outputKey.setMonth(bMonth);
                mos.write("departure", outputKey, result);
            }
        } else {
            for (IntWritable value : values) {
                if (bMonth != key.getMonth()) {
                    result.set(sum);
                    outputKey.setYear(key.getYear().substring(2));
                    outputKey.setMonth(bMonth);
                    mos.write("arrival", outputKey, result);
                    sum = 0;
                }
                sum += value.get();
                bMonth = key.getMonth();
            }
            if (key.getMonth() == bMonth) {
                result.set(sum);
                outputKey.setYear(key.getYear().substring(2));
                outputKey.setMonth(bMonth);
                mos.write("arrival", outputKey, result);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
