package com.atguigu.mr.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"D:\\input\\kvtest","D:\\output"};
        Configuration conf=new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        //1.创建job
        Job job=Job.getInstance(conf);
        //2.设置jar包路径
        job.setJarByClass(KVTextDriver.class);
        //3.关联Mapper和Reducer
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
        //4.设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //计数案例
//        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //计数案例
//        job.setOutputValueClass(IntWritable.class);
        //6.设置输入输出路径
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交
        boolean result=job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
