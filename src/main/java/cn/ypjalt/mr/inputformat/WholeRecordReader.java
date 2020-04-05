package cn.ypjalt.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit split;
    private Configuration configuration;

    private BytesWritable value = new BytesWritable();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!processed) {
            // 1定义缓存
            byte[] contents = new byte[(int) split.getLength()];
            // 2获取文件系统
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            // 3读取内容
            FSDataInputStream fis = null;
            try {
                // 3.1打开输出流
                FSDataInputStream fsDataInputStream = fs.open(path);
                IOUtils.readFully(fis, contents, 0, contents.length);
                // 3.2输出文件内容
                value.set(contents, 0, contents.length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {

                IOUtils.closeStream(fis);
            }
            processed = true;
            return true;


        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
