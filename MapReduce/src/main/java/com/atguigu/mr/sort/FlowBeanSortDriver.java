package com.atguigu.mr.sort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowBeanSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"D:\\input\\flowSort","D:\\output1"};
        Configuration conf=new Configuration();
        //1.获取Job对象
        Job job=Job.getInstance(conf);
        //2.设置jar存放路径
        job.setJarByClass(FlowBeanSortDriver.class);
        //3.关联Map和Reduce类
        job.setMapperClass(FlowBeanSortMapper.class);
        job.setReducerClass(FlowBeanSortReducer.class);
        //4.设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //5.设置最终数据输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置程序运行的输入路径和输出路径
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交Job
        boolean result=job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
