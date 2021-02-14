package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//map阶段
//KEYIN 输入数据的key 行的偏移量
//VALUEIN 输入数据的value
//KEYOUT    输出数据的类型
//VALUEOUT  输出数据的value类型
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k=new Text();
    IntWritable v=new IntWritable(1);
    /**
     * 重写map()方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //atguigu atguigu
        //1获取一行
        String line=value.toString();
        //2.切割单词
        String[] words=line.split(" ");
        //3.循环写出
        for (String word : words) {
            //atguigu
            k.set(word);
            context.write(k,v);
        }
    }
}
