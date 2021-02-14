package com.atguigu.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String[] line=value.toString().split("\t");
        //2.调用方法判断是否是有效数据
        boolean what=isValid(line,context);
        if(!what){
            return;
        }
        context.write(value, NullWritable.get());
    }

    private boolean isValid(String[] line,Context context) {
        boolean result;
        String one=line[0];
        String two=line[1];
        String three=line[2];
        String[] ones=one.split("\\.");
        //ip地址最后一段不是0和255并且请求方式是GET访问用户不是root的就是有效数据
        if(!"0".equals(ones[3])
                &&!"255".equals(ones[3])
                &&"GET".equals(two)
                &&!"root".equals(three)){
            result=true;
            context.getCounter("map","有效数据").increment(1);
        }else {
            context.getCounter("map","无效数据").increment(1);
            result=false;
        }
       return result;
    }

    public static void main(String[] args) {
        try {
            File file=new File("D:\\input\\ETL\\test.txt");
            FileOutputStream fileOutputStream=new FileOutputStream(file);
            String ip1="202.212.";
            StringBuilder str=new StringBuilder();
            for(int i=0;i<20000;i++){
            str.append(ip1).append(i/256).append(".").append(i%256).append("\t").append(i%3==0?"GET":"PUT").append("\t").append(i%30==0?"root":"user"+i).append("\r\n");

        }
            fileOutputStream.write(str.toString().getBytes());
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
