package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class HDFSIO {
    //把本地d盘上的banhua.txt文件上传到HDFS根目录
    @Test
    public void putFileToHDFS() throws Exception {
        Configuration conf=new Configuration();
        //1获取对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2获取输入流
        FileInputStream fileInputStream=new FileInputStream("D:\\banhua.txt");
        //3获取输出流
        FSDataOutputStream fsDataOutputStream=fileSystem.create(new Path("/banhua.txt"));
        //4流的对拷
        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,conf);
        //5关闭资源
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
        fileSystem.close();
    }

    //把本地HDFS上的banhua.txt文件下载到D盘
    @Test
    public void getFileFromHDFS() throws Exception {
        Configuration conf=new Configuration();
        //1获取对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2获取输入流
        FSDataInputStream fsDataInputStream=fileSystem.open(new Path("/banhua.txt"));
        //3获取输出流
        FileOutputStream fileOutputStream=new FileOutputStream("D:\\banhua.txt");
        //4流的对拷
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,conf);
        //5关闭资源
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fileOutputStream);
        fileSystem.close();
    }


    //定位读取
    //下载第一块
    @Test
    public void readFileSeek1()throws  Exception{
        Configuration conf=new Configuration();
        //1获取对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2获取输入流
        FSDataInputStream fsDataInputStream=fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        //3获取输出流
        FileOutputStream fileOutputStream=new FileOutputStream("D:\\hadoop_part1");
        //4流的对拷(只拷128M)
        byte[] buf=new byte[1024];
        for(int i=0;i<1024*128;i++){
            fsDataInputStream.read(buf);
            fileOutputStream.write(buf);
        }
        //5关闭资源
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fileOutputStream);
        fileSystem.close();
    }
    //下载第二块
    @Test
    public void readFileSeek2()throws  Exception{
        Configuration conf=new Configuration();
        //1获取对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2获取输入流
        FSDataInputStream fsDataInputStream=fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        //3设置读取起点
        fsDataInputStream.seek(1024*1024*128);
        //4获取输出流
        FileOutputStream fileOutputStream=new FileOutputStream("D:\\hadoop_part2");
        //5流的对拷
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,conf);
        //6关闭资源
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fileOutputStream);
        fileSystem.close();
        //windows 命令行拼接 type hadoop_part2 >> hadoop_part1
    }
}
