package com.learn.hadoop.delay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by inheekim on 2014. 10. 5..
 */
public class GroupKeyPartitioner extends Partitioner<DateKey, IntWritable> {
    @Override
    public int getPartition(DateKey dateKey, IntWritable intWritable, int numPartitions) {
        int hash = dateKey.getYear().hashCode();
        return hash % numPartitions;
    }
}
