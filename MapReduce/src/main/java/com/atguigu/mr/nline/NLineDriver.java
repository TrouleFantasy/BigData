package com.atguigu.mr.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NLineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"D:\\input\\NLineTest","D:\\output"};
        Configuration conf=new Configuration();
        //1.获取job
        Job job=Job.getInstance(conf);
        //2.设置jar包路径
        job.setJarByClass(NLineDriver.class);
        //3.关联map和reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        //4.设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输入输出路径
        //设置使用NLineinputFormat处理数据
        job.setInputFormatClass(NLineInputFormat.class);
        //设置每个切片划分的行数
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交
        boolean result=job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
