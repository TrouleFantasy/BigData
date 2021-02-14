package com.atguigu.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyFilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosatguigu;
    FSDataOutputStream fosother;
    public MyFilterRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {
            //1.获取文件系统
            FileSystem fs=FileSystem.get(taskAttemptContext.getConfiguration());
            //2.创建输出到atguigu.log的输出流
            fosatguigu =fs.create(new Path("D:\\atguigu.log"));
            //3.创建输出到other.log的输出流
            fosother =fs.create(new Path("D:\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //判断key当中是否有atguigu 有写出到atguigu.log 否则other.log
        if(text.toString().contains("atguigu")){
            //输出到atguigu
            fosatguigu.write(text.toString().getBytes());
        }else{
            //输出到other
            fosother.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fosatguigu);
        IOUtils.closeStream(fosother);
    }
}
