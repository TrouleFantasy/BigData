package com.atguigu.mr.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
    public static void main(String[] args) throws Exception{
        args=new String[]{"D:\\input\\Order","D:\\output"};
        System.out.println("1:"+args[0]+" 2:"+args[1]);

        Configuration conf=new Configuration();
        //1.获取Job对象
        Job job=Job.getInstance(conf);

        //2.设置jar存放路径
//        job.setJar("");//此方式会写死路径 相当于绝对路径
        job.setJarByClass(OrderDriver.class);//此方式比较灵活 相当于相对路径 利用反射原理找到该类位置

        //3.关联Map和Reduce类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //4.设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终数据输出的key和value
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置程序运行的输入路径和输出路径
        //设置分组排序比较器
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7.提交Job
//        job.submit();
        boolean result =job.waitForCompletion(true);//为true 提交完成后有打印信息 否则无
        //运行成功打印0否则1 linux系统中 可以在运行完以后 使用echo $?来查看
        System.exit(result?0:1);
    }

}
