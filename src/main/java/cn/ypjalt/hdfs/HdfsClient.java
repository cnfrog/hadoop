package cn.ypjalt.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        // 配置在集群上运行
//        conf.set("fs.defaultFS","hdfs://hadoop102:9000");
//        FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");
        // 2.创建目录
        fs.mkdirs(new Path("/user/hadoop"));
        // 3.关闭资源
        fs.close();

    }
}
