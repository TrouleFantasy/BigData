package com.atguigu.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    FlowBean k=new FlowBean();
    Text v=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //13228791465	781	1234	2015
        //1.获取一行
        String line=value.toString();
        //2.切割
        String[] lines=line.split("\t");
        //3.封装
        v.set(lines[0]);
        long upFlow=Long.parseLong(lines[1]);
        long downFlow=Long.parseLong(lines[2]);
        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(upFlow+downFlow);
        //4.提交
        context.write(k,v);
    }
}
