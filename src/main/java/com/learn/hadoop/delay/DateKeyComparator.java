package com.learn.hadoop.delay;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by inheekim on 2014. 10. 5..
 */
public class DateKeyComparator extends WritableComparator {
    protected DateKeyComparator() {
        super(DateKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateKey k1 = (DateKey) a;
        DateKey k2 = (DateKey) b;

        int cmp = k1.getYear().compareTo(k2.getYear());
        if (cmp != 0) {
            return cmp;
        }

        return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
    }
}
