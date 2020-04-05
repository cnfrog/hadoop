package cn.ypjalt.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream aOut = null;
    FSDataOutputStream bOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        // 1获取文件系统、
        FileSystem fs;

        try {
            fs = FileSystem.get(job.getConfiguration());
            // 2创建文件输出路径
            Path aPath = new Path("E://ee");
            Path bPath = new Path("E://ee");
            // 3 创建输出流
            aOut = fs.create(aPath);
            bOut = fs.create(bPath);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        if (key.toString().contains("atigui")) {
            aOut.write(key.toString().getBytes());
        } else {
            bOut.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关闭资源
        if (aOut != null) {
            aOut.close();
        }

        if (bOut != null) {
            bOut.close();
        }

    }
}
