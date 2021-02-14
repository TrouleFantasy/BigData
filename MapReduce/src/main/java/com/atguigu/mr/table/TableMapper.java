package com.atguigu.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    TableBean v =new TableBean();
    Text k=new Text();
    String fileName;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件的名称
        FileSplit fileSplit=(FileSplit)context.getInputSplit();
        fileName=fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1001	01	1
        //1002	02	2

        //01	小米
        //02	华为
        //03	格力

        //1.获取一行
        String line=value.toString();
        //2.切割
        String[] lines=line.split("\t");
        //判断是哪张表
        if(fileName.startsWith("order")){
            //3.封装
            v.setId(lines[0]);
            v.setPid(lines[1]);
            v.setAmount(Integer.parseInt(lines[2]));
            v.setPname("");
            v.setFlag("order");
        }else{
            //3.封装
            v.setId("");
            v.setPid(lines[0]);
            v.setAmount(0);
            v.setPname(lines[1]);
            v.setFlag("pd");
        }
        //4.写出
        k.set(v.getPid());
        context.write(k,v);
    }
}
