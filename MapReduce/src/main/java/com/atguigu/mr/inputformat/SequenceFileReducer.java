package com.atguigu.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SequenceFileReducer extends Reducer<Text, BytesWritable,Text,BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        //循环写出 事实证明本案例中 这个for只会循环一次
        int a=1;
        for (BytesWritable value : values) {
//            System.out.println("ReducerKey:"+a+key);
//            System.out.println("ReducerValue:"+new String(value.getBytes()));
            context.write(key, value);
            a++;
        }

    }
}
