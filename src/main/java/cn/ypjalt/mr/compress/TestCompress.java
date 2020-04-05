package cn.ypjalt.mr.compress;


import org.apache.hadoop.io.CompressedWritable;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class TestCompress {

    public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException {
        compress("/","org.");
    }

    private static void compress(String filename, String method) throws FileNotFoundException, ClassNotFoundException {
        // 1 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        Class codeClass = Class.forName(method);

//        CompressionCodec cos =

        // 2.获取输出流
//        new FileOutputStream(new File(filename+))
        // 3.流的对烤

    }
}
