package com.atguigu.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean, NullWritable> {
    public OrderReducer(){
        System.out.println("我是reduce");
    }
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //写出

        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
