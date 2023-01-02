package com.song.hadoopdemo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * HDFS客户端测试类
 * 官网地址： https://hadoop.apache.org/docs/r3.1.3/
 */
public class HdfsClientTest {

    //文件系统
    private FileSystem fs;

    /**
     * 方法调用之前执行
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // hdfs://hadoop102:8020 是nameNode的通信地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //  获取文件系统配置
        Configuration configuration = new Configuration();
        // 定义具有操作权限的用户
        String user = "song";
        //  获取客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    /**
     * 方法调用之后执行
     *
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        // 关闭资源
        fs.close();
    }

    /**
     * 测试在HDFS上创建目录
     *
     * @throws IOException
     */
    @Test
    public void testMkdirs() throws IOException {
        // 在HDFS上创建目录
//        fs.mkdirs(new Path("/huaru"));
//        fs.mkdirs(new Path("/testRemove"));
        fs.mkdirs(new Path("/testMove"));
    }

    /**
     * 测试本地文件上传到HDFS
     */
    @Test
    public void testPut() throws IOException {
        /* *
         * 第一个参数：是否删除源文件
         * 第二个参数：目标文件存在，是否覆盖
         * 第三个参数：源文件地址
         * 第四个参数：目标文件地址
         * 参数优先级  从左到右 从低到高
         * hdfs-default.xml  ==>  hdfs-site.xml ==> 在项目资源目录下的配置文件 ==> 代码里面的配置
         */
//        fs.copyFromLocalFile(false, true, new Path("D:\\test_data\\input\\wordCount.txt"), new Path("/huaru/upload/wordCount.txt"));
//        fs.copyFromLocalFile(false, true, new Path("D:\\test_data\\input\\wordCount.txt"), new Path("/testRemove/wordCount.txt"));
        fs.copyFromLocalFile(false, true, new Path("D:\\wordCount2.txt"), new Path("/testRemove/wordCount2.txt"));
    }


    /**
     * 测试从HDFS下载到本地
     *
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {

        /* *
         * 第一个参数：是否删除源文件
         * 第二个参数：源文件地址
         * 第三个参数：目标文件地址
         * 第四个参数：是否开启文件校验
         * 参数优先级  从左到右 从低到高
         * hdfs-default.xml  ==>  hdfs-site.xml ==> 在项目资源目录下的配置文件 ==> 代码里面的配置
         */
        fs.copyToLocalFile(false, new Path("/huaru/upload/wordCount.txt"), new Path("D:\\test_data\\download\\wordCount.txt"), false);
    }

    /**
     * 测试删除HDFS中的数据文件
     */
    @Test
    public void testRemove() throws IOException {

        /* *
         * 第一个参数：是否删除源文件
         * 第二个参数：源文件地址
         */
        fs.delete(new Path("/testRemove"), true);
    }


    /**
     * 测试HDFS 移动数据
     *
     * @throws IOException
     */
    @Test
    public void testMove() throws IOException {

        // 业务操作
        /* *
         * 第一个参数：源文件
         * 第二个参数：目标文件
         */
        fs.rename(new Path("/huaru/upload/wordCount.txt"), new Path("/testMove"));
//        fs.rename(new Path("/testMove"), new Path("/huaru/upload/wordCount.txt"));
    }


    /**
     * 从HDFS中获取文件详情信息
     *
     * @throws IOException
     */
    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            // 文件的读写权限信息 rw-r--r--
            System.out.println(fileStatus.getPermission());
            // 文件的拥有者  song
            System.out.println(fileStatus.getOwner());
            // 文件的分组  supergroup
            System.out.println(fileStatus.getGroup());
            // 文件的长度 18
            System.out.println(fileStatus.getLen());
            // 文件的最后修改时间 1670483788325
            System.out.println(fileStatus.getModificationTime());
            // 文件的副本数 3
            System.out.println(fileStatus.getReplication());
            // 文件的块大小
            System.out.println(fileStatus.getBlockSize());
            // 文件所在快的大小 134217728/1024/1024=128M
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息 [0,134217728,hadoop104,hadoop103,hadoop102]
            // 0:代表位置偏移量的起始位置 134217728 代表占用的字节数，hadoop104,hadoop103,hadoop102：代表备份数据的实例服务器
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }
}


