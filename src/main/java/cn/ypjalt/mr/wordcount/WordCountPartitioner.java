package cn.ypjalt.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//把单词按照ASCII码奇偶分区（Partitioner）
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int i) {
        String substring = key.toString().substring(0, 1);
        char[] chars = substring.toCharArray();
        int result = chars[0];
        if (result % 2 == 0) {
            return 1;
        }
        return 0;
    }
}
