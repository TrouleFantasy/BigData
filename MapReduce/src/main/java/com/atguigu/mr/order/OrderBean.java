package com.atguigu.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int orderId;//订单id
    private double price;//订单价格
    public OrderBean(){

    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean bean) {
        int result=0;
        //先按照订单id进行排序
        if(orderId<bean.getOrderId()){
            result=-1;
        }else if(orderId>bean.getOrderId()){
            result=1;
        }else {
            //如果订单相同则按照价格排序
            if(price>bean.getPrice()){
                result=-1;
            }else if(price<bean.getPrice()){
                result=1;
            }
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId=dataInput.readInt();
        price=dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price ;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
