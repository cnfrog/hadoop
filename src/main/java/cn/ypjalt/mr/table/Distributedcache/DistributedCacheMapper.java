package cn.ypjalt.mr.table.Distributedcache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Map<String, String> pdMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException {
        // 1 获取缓存的文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("pd.txt"), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 2 切割
            String[] fields = line.split("\t");
            // 3 缓存数据到集合
            pdMap.put(fields[0], fields[1]);
        }
        reader.close();

    }

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String pId = fields[1];
        String pdName = pdMap.get(pId);
        // 5 拼接
        k.set(line + "\t" + pdName);
        // 6 写出
        context.write(k, NullWritable.get());

    }
}
