package com.atguigu.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text phoneNum=new Text();
    FlowBean flowBean=new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //d		手机号码		网络ip	域名		上行流量		下行流量		网络状态码
        //0	13560436666	120.196.100.99     	1116	954	200
        //1.获取一行
        String line=value.toString();
        //2.拆分
        String[] lines=line.split("\t");
        //3.封装对象
        phoneNum.set(lines[1]);
        //因为域名列不一定会有 所以 从后面取上下行流量
        Long upFlow=Long.parseLong(lines[lines.length-3]);
        Long downFlow=Long.parseLong(lines[lines.length-2]);
        flowBean.setUpLow(upFlow);
        flowBean.setDownLow(downFlow);
//        flowBean.set(upFlow,downFlow);
        //4.将手机号作为key 封装上下行流量的bean对象作为value写入到上下文
        context.write(phoneNum,flowBean);
    }
}
