package com.atguigu.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable{
    private long upLow;//上行流量
    private long downLow;//下行流量
    private long sumLow;//总流量

    //空参构造，为了后续反射用
    public FlowBean(){

    }

    public FlowBean(long upLow,long downLow){
        this.upLow=upLow;
        this.downLow=downLow;
        this.sumLow=upLow+downLow;
    }
    //序列化方法
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upLow);
        dataOutput.writeLong(downLow);
        dataOutput.writeLong(sumLow);
    }
    //反序列化方法
    public void readFields(DataInput dataInput) throws IOException {
        //必须要求和序列化方法顺序一致
        upLow=dataInput.readLong();
        downLow=dataInput.readLong();
        sumLow=dataInput.readLong();
    }

    @Override
    public String toString() {
        return upLow +
                "\t" + downLow +
                "\t" + sumLow ;
    }

    public long getUpLow() {
        return upLow;
    }

    public void setUpLow(long upLow) {
        this.upLow = upLow;
    }

    public long getDownLow() {
        return downLow;
    }

    public void setDownLow(long downLow) {
        this.downLow = downLow;
    }

    public long getSumLow() {
        return sumLow;
    }

    public void setSumLow(long sumLow) {
        this.sumLow = sumLow;
    }
    public void set(long upLow,long downLow){
        this.upLow=upLow;
        this.downLow=downLow;
        this.sumLow=upLow+downLow;
    }
    public void setAddFlow(long upLow,long downLow){
        this.upLow+=upLow;
        this.downLow+=downLow;
        this.sumLow+=upLow+downLow;
    }
}
