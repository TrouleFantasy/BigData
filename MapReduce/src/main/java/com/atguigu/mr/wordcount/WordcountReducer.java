package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//KEYIN,VALUEIN map阶段输出的KV
//KEYOUT
//VALUEOUT
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v=new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //经过上一个mapper处理之后 此时的数据如此
        //atguigu,1
        //atguigu,1
        //所以values是一个IntWritable的迭代器 里面有很多个1

        //1.累加求和
        int sum=0;
        for (IntWritable value : values) {
            System.out.println("key:"+key+" value:"+value.get());
            //获取int类型
            sum+=value.get();
        }
        v.set(sum);
        //2.写出 atguigu 2
        context.write(key,v);
    }
}
