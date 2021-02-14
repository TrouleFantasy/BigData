package com.atguigu.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
    IntWritable v=new IntWritable();
    Text k=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line=value.toString();
        //2.切割
        String[] lines=line.split("\t");
        //3.循环写出
        v.set(1);
        for (String s : lines) {
            k.set(s);
            context.write(k, v);
        }
    }
}
