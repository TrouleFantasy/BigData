package com.atguigu.mr.kv;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KVTextMapper extends Mapper<Text,Text,Text, Text> {
//    IntWritable intWritable=new IntWritable(1);
        Text v=new Text();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //小菊 3欺负 三狼
        //1.直接提交
        context.write(key, value);

        //计数案例
        //直接提交 没错 就是直接提交
//        context.write(key, intWritable);
    }
}
