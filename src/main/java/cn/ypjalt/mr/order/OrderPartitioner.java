package cn.ypjalt.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;


public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

    public int getPartition(OrderBean key, NullWritable value, int i) {

        return (key.getOrder_id() & Integer.MAX_VALUE) % i;
    }

    public void configure(JobConf jobConf) {

    }
}
