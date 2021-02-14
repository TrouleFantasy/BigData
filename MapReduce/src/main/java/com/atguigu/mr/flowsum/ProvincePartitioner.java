package com.atguigu.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int i) {
        //key是手机号
        //value 是流量信息
        //要求按照手机号前三位来分类 152 137 136 135 其他
        String prePhoneNum=key.toString().substring(0,3);
        int partition=4;
        if("152".equals(prePhoneNum)){
            partition=0;
        }else if("137".equals(prePhoneNum)){
            partition=1;
        }else if("136".equals(prePhoneNum)){
            partition=2;
        }else if("135".equals(prePhoneNum)){
            partition=3;
        }
        return partition;
    }
}
