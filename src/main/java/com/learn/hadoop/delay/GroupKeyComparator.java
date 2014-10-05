package com.learn.hadoop.delay;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by inheekim on 2014. 10. 5..
 */
public class GroupKeyComparator extends WritableComparator {

    protected GroupKeyComparator() {
        super(DateKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateKey k1 = (DateKey) a;
        DateKey k2 = (DateKey) b;

        return k1.getYear().compareTo(k2.getYear());
    }
}
