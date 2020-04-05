package cn.ypjalt.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();

        for (Text value : values
        ) {
            sb.append(value.toString().replace("\t", "-->") + "\t");

        }

        v.set(sb.toString());
        // 写出
        context.write(key, v);
    }
}
