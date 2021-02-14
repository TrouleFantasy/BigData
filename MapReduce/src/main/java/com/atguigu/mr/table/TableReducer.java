package com.atguigu.mr.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderBeans=new ArrayList<>();
        TableBean pd=new TableBean();
        //循环替换并写出
        for (TableBean value : values) {
            if("order".equals(value.getFlag())){
                TableBean tmpbean=new TableBean();
                try {
                    BeanUtils.copyProperties(tmpbean, value);
                    orderBeans.add(tmpbean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else {
                try {
                    BeanUtils.copyProperties(pd, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

            //遍历order做pname复制
            for (TableBean orderBean : orderBeans) {
                orderBean.setPname(pd.getPname());
                //写出
                context.write(orderBean,NullWritable.get());
            }
        }
    }
}
