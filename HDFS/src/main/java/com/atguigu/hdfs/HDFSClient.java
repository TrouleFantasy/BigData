package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws Exception, URISyntaxException {
        Configuration conf=new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop102:9000");

        //1.获取hdfs客户端对象
//        FileSystem fileSystem = FileSystem.get(conf);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        //2在hdfs上创建路径
        fileSystem.mkdirs(new Path("/0529/dashen"));
        //3关闭资源
        fileSystem.close();
        System.out.println("over");
    }
    //1文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf=new Configuration();
        conf.set("dfs.replication","2");
        //1获取fs对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2执行上传API
        fileSystem.copyFromLocalFile(new Path("D:\\banzhang.txt"),new Path("/xiaohua.txt"));
        //3关闭资源
        fileSystem.close();
    }
    //2文件下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf=new Configuration();
        //1获取fs对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2执行下载API
//        fileSystem.copyToLocalFile(new Path("/banhua.txt"),new Path("D:\\banhua.txt"));
        //是否删除原数据 源 目标 是否本地模式(false会校验，会产生CRC文件)
        fileSystem.copyToLocalFile(false,new Path("/banhua.txt"),new Path("D:\\banhua.txt"),true);
        //3关闭资源
        fileSystem.close();
    }
    //3文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf=new Configuration();
        //1获取fs对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2执行删除API
        //目标,是否是文件夹
        fileSystem.delete(new Path("/0529"),true);
        //3关闭资源
        fileSystem.close();
    }
    //4文件更名
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf=new Configuration();
        //1获取fs对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2执行更名API
        fileSystem.rename(new Path("/banzhang.txt"),new Path("/yanjing.txt"));
        //3关闭资源
        fileSystem.close();
    }
    //5文件详情查看
    @Test
    public void testListFiles()throws Exception{
        Configuration conf=new Configuration();
        //1获取fs对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2查看文件详情
        //获取/目录下所有文件
        RemoteIterator<LocatedFileStatus> listFiles=fileSystem.listFiles(new Path("/"),true);
        //取出每一个文件
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus=listFiles.next();
            System.out.println("文件名称："+fileStatus.getPath().getName());
            System.out.println("文件权限："+fileStatus.getPermission());
            System.out.println("文件长度："+fileStatus.getLen());
            //获取回块位置信息数组 文件被分成了几块数组长度就是几
            BlockLocation[] blockLocations=fileStatus.getBlockLocations();
            System.out.println("块："+blockLocations.length);
            for (BlockLocation blockLocation : blockLocations) {
                //获取这个块在哪些主机上有存在
                String[] hosts=blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("文件块信息："+host);
                }
            }
            System.out.println("---------------------------------------------------------------");
        }
        //3关闭资源
        fileSystem.close();
    }
    //6判断是文件还是文件夹
    @Test
    public void testListStatus()throws Exception{
        Configuration conf=new Configuration();
        //1获取fs对象
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2判断操作
        //获取所有文件以及文件夹的信息
        FileStatus[] listStatus=fileSystem.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()){
                //文件
                System.out.println("f:"+status.getPath().getName());
            }else {
                //文件夹
                System.out.println("d:"+status.getPath().getName());
            }
        }
        //3关闭资源
        fileSystem.close();
    }
}
