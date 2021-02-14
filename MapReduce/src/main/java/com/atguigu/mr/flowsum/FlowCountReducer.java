package com.atguigu.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //手机号码	上行流量	下行流量	总流量
        //15233571928	713     1999    X
        //15233571928	1999    713     X

        //1.累加求和
        FlowBean v=new FlowBean();
        for (FlowBean value : values) {
            v.setAddFlow(value.getUpLow(), value.getDownLow());
        }
        //2.写出
        context.write(key, v);
    }

}
