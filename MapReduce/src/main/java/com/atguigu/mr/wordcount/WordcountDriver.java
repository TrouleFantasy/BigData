package com.atguigu.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordcountDriver {
    //对一个任务的封装 此步骤就是给一个任务设置好运行必须的东西 保证任务的运行 比如 输入输出路径 用哪个Map和Reduce 输出类型等等
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"D:\\input\\Text","D:\\output"};
        System.out.println("1:"+args[0]+" 2:"+args[1]);
        Configuration conf=new Configuration();
        //开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress",true);
        //设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        //1.获取Job对象
        Job job=Job.getInstance(conf);

        //2.设置jar存放路径
//        job.setJar("");//此方式会写死路径 相当于绝对路径
        job.setJarByClass(WordcountDriver.class);//此方式比较灵活 相当于相对路径 利用反射原理找到该类位置

        //3.关联Map和Reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //4.设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终数据输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置reduce端输出压缩
        FileOutputFormat.setCompressOutput(job, true);
        //设置reduce压缩方式
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //6.设置程序运行的输入路径和输出路径
        //更改job使用的InputFormat 默认是TextInputFormat
//        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置虚拟储存切片最大值
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        //设置Combiner
//        job.setCombinerClass(WordCountCombiner.class);
        //设置分区数
//        job.setNumReduceTasks(2);
         FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交Job
//        job.submit();
        boolean result =job.waitForCompletion(true);//为true 提交完成后有打印信息 否则无
        //运行成功打印0否则1 linux系统中 可以在运行完以后 使用echo $?来查看
        System.exit(result?0:1);
    }

}
