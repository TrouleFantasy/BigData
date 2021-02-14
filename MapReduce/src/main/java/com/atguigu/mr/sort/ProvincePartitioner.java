package com.atguigu.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean bean, Text text, int i) {
        String phoneNum=text.toString().substring(0,3);
        int partition=4;
        if("152".equals(phoneNum)){
            partition=0;
        }else if("137".equals(phoneNum)){
            partition=1;
        }else if("136".equals(phoneNum)){
            partition=2;
        }else if("135".equals(phoneNum)){
            partition=3;
        }
        return partition;
    }
}
