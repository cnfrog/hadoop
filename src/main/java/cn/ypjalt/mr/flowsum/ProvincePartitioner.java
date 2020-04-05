package cn.ypjalt.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;


// 将统计结果按照手机归属地不同省份输出到不同文件中（Partitioner）
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public int getPartition(Text key, FlowBean value, int i) {
        String preNum = key.toString().substring(0, 3);
        int partition = 4;
        if ("136".equals(preNum))
            partition = 0;
        else if ("137".equals(preNum))
            partition = 1;
        else if ("138".equals(preNum))
            partition = 2;
        else if ("139".equals(preNum))
            partition = 3;
        return partition;
    }

    public void configure(JobConf jobConf) {

    }
}
