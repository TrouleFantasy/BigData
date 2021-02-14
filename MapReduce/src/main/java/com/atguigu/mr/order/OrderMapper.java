package com.atguigu.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    public OrderMapper(){
        System.out.println("我是Mapper");
    }
    OrderBean orderBean=new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //000001	o1	300
        //获取一行并切割
        String[] lines=value.toString().split("\t");
        //封装
        Integer id=Integer.parseInt(lines[0]);
        Double price=Double.parseDouble(lines[2]);
        orderBean.setOrderId(id);
        orderBean.setPrice(price);
        //写出
        context.write(orderBean, NullWritable.get());
    }
}
