package com.atguigu.mr.cache;

import com.atguigu.mr.table.TableBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.util.HashMap;

public class DistrbutrdCacheMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private String fn;
    //产品表
    private HashMap<String,String> pdMap=new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名
        FileSplit f=(FileSplit)context.getInputSplit();
        fn=f.getPath().getName();
        //缓存小表
        String filePath=context.getCacheFiles()[0].getPath();
        System.out.println("!!!:"+filePath);
        BufferedReader buf=new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line;
        while (StringUtils.isNotEmpty(line=buf.readLine())){
            //1切割
            String[] lines=line.split("\t");
            //2 下标0是pid 1是pname
            pdMap.put(lines[0],lines[1]);
        }
        //2关闭资源
        IOUtils.closeStream(buf);
    }
    TableBean bean=new TableBean();
    Text k=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1001	01	1
        //1002	02	2

        //1.获取一行
            String line=value.toString();
        //2.切割
            String[] lines=line.split("\t");
        //3.封装
        if(fn.startsWith("order")){
            bean.setId(lines[0]);
            bean.setPid(lines[1]);
            bean.setAmount(Integer.parseInt(lines[2]));
            bean.setPname(pdMap.get(lines[1]));
            bean.setFlag("order");
            k.set(bean.toString());
            //4.写出
            context.write(k, NullWritable.get());
        }else {
            k.set("");
            context.write(k, NullWritable.get());
        }

    }
}
