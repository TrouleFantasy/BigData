package com.atguigu.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    FileSplit split;
    Configuration configuration;
    Text k=new Text();
    BytesWritable v=new BytesWritable();
    boolean isProgress=true;
    //初始化
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split=(FileSplit)inputSplit;
//        System.out.println("inputSplit::"+inputSplit);
        configuration=taskAttemptContext.getConfiguration();
    }

    //核心业务逻辑
    public boolean nextKeyValue() throws IOException, InterruptedException {
       if(isProgress){
           byte[] buf=new byte[(int)split.getLength()];
           //1.获取fs对象
           Path path=split.getPath();
           FileSystem fs=path.getFileSystem(configuration);
           //2.获取输入流
           FSDataInputStream fis=fs.open(path);
           //3.拷贝
           //源 读入哪 从哪读 读多少
           IOUtils.readFully(fis, buf, 0,buf.length );
           //4.封装v
           v.set(buf, 0, buf.length);
           //5.封装k
           k.set(path.toString());
           //6.关闭资源
           IOUtils.closeStream(fis);
           isProgress=false;
           return true;
       }
       return false;
    }

    //获取当前key
    public Text getCurrentKey() throws IOException, InterruptedException {

        return k;
    }
    //获取档期value
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {

        return v;
    }


    //获取正在处理的进程 进度条
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }
    //关闭资源
    public void close() throws IOException {

    }
}
