package com.atguigu.mr.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //压缩
//        compress("D:\\input\\ETL\\test.txt","org.apache.hadoop.io.compress.BZip2Codec");
//        compress("D:\\input\\ETL\\test.txt","org.apache.hadoop.io.compress.GzipCodec");
//        compress("D:\\input\\ETL\\test.txt","org.apache.hadoop.io.compress.DefaultCodec");

        //解压缩
//        deCompress("D:\\input\\ETL\\test.txt.bz2");
//        deCompress("D:\\input\\ETL\\test.txt.deflate");
//        deCompress("D:\\input\\ETL\\test.txt.gz");
    }

    private static void deCompress(String path) throws IOException {
        //合法性检查
        CompressionCodecFactory factory=new CompressionCodecFactory(new Configuration());
        CompressionCodec codec=factory.getCodec(new Path(path));
        if(codec==null){
            System.out.println("no!");
            return;
        }
        //1.获取输入流
        FileInputStream fis=new FileInputStream(path);
        CompressionInputStream cis=codec.createInputStream(fis);
        //2.获取输出流
        FileOutputStream fos=new FileOutputStream(path+".decode");
        //3.流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024, false);
        //4.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }

    public static void compress(String path,String classpath) throws IOException, ClassNotFoundException {
        //1.获取输入流
        FileInputStream fis=new FileInputStream(path);
        //2.获取输出流
        Class clazz=Class.forName(classpath);
        CompressionCodec codec =(CompressionCodec)ReflectionUtils.newInstance(clazz, new Configuration());
        FileOutputStream fos=new FileOutputStream(path+codec.getDefaultExtension());
        //3.流的对拷
        CompressionOutputStream cos=codec.createOutputStream(fos);
        //最后的false代表是否在操作完成后关闭输入输出流
        IOUtils.copyBytes(fis, cos, 1024*1024, false);
        //4.关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }



}
