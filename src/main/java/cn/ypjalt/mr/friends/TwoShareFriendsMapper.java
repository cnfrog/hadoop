package cn.ypjalt.mr.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TwoShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // A I,K,C,B,G,F,H,O,D,
        // 友 人，人，人
        String line = value.toString();
        String[] friends_person = line.split("\t");
        String friend = friends_person[0];
        String[] persons = friends_person[1].split(",");
        Arrays.sort(persons);
        for (int i = 0; i < persons.length - 1; i++)
            for (int j = i + 1; j < persons.length; j++)
                context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
    }
}
