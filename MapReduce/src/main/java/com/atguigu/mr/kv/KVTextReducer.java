package com.atguigu.mr.kv;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVTextReducer extends Reducer<Text,Text, Text, Text> {
//    IntWritable v=new IntWritable();
        Text v=new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //1.实现排序

        //2.拼接
        StringBuffer line=new StringBuffer();
        for (Text value : values) {
            line.append(value);
        }
        v.set(line.toString());
        //2.写出
        context.write(key, v);
        //计数案例
        //1.累加求和
//        int sum=0;
//        for (IntWritable value : values) {
//            sum+=value.get();
//        }
//        v.set(sum);
//        //2.写出
//        context.write(key, v);

    }
}
