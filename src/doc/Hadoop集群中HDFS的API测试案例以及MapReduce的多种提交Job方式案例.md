
**这两个案例默认是hadoop集群环境已经搭建好以及IDEA环境也已经配置好**
## 1、HDFS客户端测试案例
### 1.1、pom依赖

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.5.1</version>
        <relativePath/> <!-- lookup parent from repository -->
    </parent>
    <groupId>com.song</groupId>
    <artifactId>hadoopdemo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>hadoopdemo</name>
    <description>Demo project for Spring Boot</description>
    <properties>
        <java.version>1.8</java.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
            <!--     排除的原因  避免出现下面的报错，虽然不影响使用，但是闹心
                org.apache.hadoop.fs.FileSystem - NativeIO.createDirectoryWithMode error, path = D:\test_data\download, mode = 755
                org.apache.hadoop.io.nativeio.NativeIOException: 当文件已存在时，无法创建该文件。
            -->
            <!--            <exclusions>-->
            <!--                <exclusion>-->
            <!--                    <groupId>ch.qos.logback</groupId>-->
            <!--                    <artifactId>logback-classic</artifactId>-->
            <!--                </exclusion>-->
            <!--                <exclusion>-->
            <!--                    <groupId>ch.qos.logback</groupId>-->
            <!--                    <artifactId>logback-core</artifactId>-->
            <!--                </exclusion>-->
            <!--            </exclusions>-->
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.30</version>
        </dependency>

    </dependencies>

    <!-- 打成两个包 一个带依赖  一个不带依赖-->
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.6.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
### 1.2、打包方式

```xml
 <!-- 打成两个包 一个带依赖  一个不带依赖-->
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.6.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```
### 1.3、日志配置
在项目的src/main/resources目录下，新建一个文件，命名为“log4j.properties”，在文件中填入以下内容
```xml
log4j.rootLogger=INFO, stdout  
log4j.appender.stdout=org.apache.log4j.ConsoleAppender  
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout  
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - %m%n  
log4j.appender.logfile=org.apache.log4j.FileAppender  
log4j.appender.logfile.File=target/spring.log  
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout  
log4j.appender.logfile.layout.ConversionPattern=%d %p [%c] - %m%n
```

### 1.4、代码实现

```java
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
        fs.copyFromLocalFile(false, true, new Path("D:\\test_data\\input\\资料.zip"), new Path("/testRemove/资料.zip"));
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
```

## 2、MapReduce提交Job的方式案例
### 2.1、需求
在给定的文本文件中统计输出每一个单词出现的总次数
### 2.2、文件格式
```xml
ss ss
zz zx
lh zx
```
### 2.3、输出格式
```xml
lh	1
ss	2
zx	2
zz	1
```
### 2.4、提交方式
#### 2.4.1、将程序打成jar包，提交到集群环境上进行测试
##### 2.4.1.1、pom依赖

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.5.1</version>
        <relativePath/> <!-- lookup parent from repository -->
    </parent>
    <groupId>com.song</groupId>
    <artifactId>hadoopdemo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>hadoopdemo</name>
    <description>Demo project for Spring Boot</description>
    <properties>
        <java.version>1.8</java.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
            <!--     排除的原因  避免出现下面的报错，虽然不影响使用，但是闹心
                org.apache.hadoop.fs.FileSystem - NativeIO.createDirectoryWithMode error, path = D:\test_data\download, mode = 755
                org.apache.hadoop.io.nativeio.NativeIOException: 当文件已存在时，无法创建该文件。
            -->
            <!--            <exclusions>-->
            <!--                <exclusion>-->
            <!--                    <groupId>ch.qos.logback</groupId>-->
            <!--                    <artifactId>logback-classic</artifactId>-->
            <!--                </exclusion>-->
            <!--                <exclusion>-->
            <!--                    <groupId>ch.qos.logback</groupId>-->
            <!--                    <artifactId>logback-core</artifactId>-->
            <!--                </exclusion>-->
            <!--            </exclusions>-->
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.30</version>
        </dependency>

    </dependencies>

    <!-- 打成两个包 一个带依赖  一个不带依赖-->
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.6.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
##### 2.4.1.2、日志配置
在项目的src/main/resources目录下，新建一个文件，命名为“log4j.properties”，在文件中填入以下内容
```xml
log4j.rootLogger=INFO, stdout  
log4j.appender.stdout=org.apache.log4j.ConsoleAppender  
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout  
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - %m%n  
log4j.appender.logfile=org.apache.log4j.FileAppender  
log4j.appender.logfile.File=target/spring.log  
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout  
log4j.appender.logfile.layout.ConversionPattern=%d %p [%c] - %m%n
```
##### 2.4.1.3、代码实现
按照MapReduce编程规范，分别编写Mapper，Reducer，Driver

###### 2.4.1.3.1、map阶段
```java
package com.song.hadoopdemo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map阶段
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
```
###### 2.4.1.3.2、reduce阶段
```java
package com.song.hadoopdemo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    int sum;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2 输出
        v.set(sum);
        context.write(key,v);
    }

}
```

###### 2.4.1.3.3、driver阶段
```java
package com.song.hadoopdemo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Demo需求: 在给定的文本文件中统计输出每一个单词出现的总次数
 * 文本格式如下：
 * banzhang
 * xuexi
 * hadoop
 * hadoop
 * <p>
 * 预期输出格式如下：
 * banzhang	1
 * hadoop	2
 * xuexi	1
 *
 *
 * 集群上打包测试命令行：
 * hadoop jar wc.jar com.song.hadoopdemo.mapreduce.WordCountDriver /testRemove/wordCount.txt /output
 *
 * /testRemove/wordCount.txt  /output
 * HDFS的输入和输出路径
 *
 *
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径  扔服务器上测试
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
```
###### 2.4.1.3.4、放到集群上
![在这里插入图片描述](https://img-blog.csdnimg.cn/b65aab427f2b41c58fbb43862940a897.png)
###### 2.4.1.3.4、执行任务
```xml
hadoop jar wc.jar com.song.hadoopdemo.mapreduce.WordCountDriver /testRemove/wordCount.txt /output
```

#### 2.4.2、在Windows上向集群提交任务进行测试
##### 2.4.2.1、pom依赖

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.5.1</version>
        <relativePath/> <!-- lookup parent from repository -->
    </parent>
    <groupId>com.song</groupId>
    <artifactId>hadoopdemo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>hadoopdemo</name>
    <description>Demo project for Spring Boot</description>
    <properties>
        <java.version>1.8</java.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
            <!--     排除的原因  避免出现下面的报错，虽然不影响使用，但是闹心
                org.apache.hadoop.fs.FileSystem - NativeIO.createDirectoryWithMode error, path = D:\test_data\download, mode = 755
                org.apache.hadoop.io.nativeio.NativeIOException: 当文件已存在时，无法创建该文件。
            -->
            <!--            <exclusions>-->
            <!--                <exclusion>-->
            <!--                    <groupId>ch.qos.logback</groupId>-->
            <!--                    <artifactId>logback-classic</artifactId>-->
            <!--                </exclusion>-->
            <!--                <exclusion>-->
            <!--                    <groupId>ch.qos.logback</groupId>-->
            <!--                    <artifactId>logback-core</artifactId>-->
            <!--                </exclusion>-->
            <!--            </exclusions>-->
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.30</version>
        </dependency>

    </dependencies>

    <!-- 打成两个包 一个带依赖  一个不带依赖-->
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.6.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
##### 2.4.2.2、日志配置
在项目的src/main/resources目录下，新建一个文件，命名为“log4j.properties”，在文件中填入以下内容
```xml
log4j.rootLogger=INFO, stdout  
log4j.appender.stdout=org.apache.log4j.ConsoleAppender  
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout  
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - %m%n  
log4j.appender.logfile=org.apache.log4j.FileAppender  
log4j.appender.logfile.File=target/spring.log  
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout  
log4j.appender.logfile.layout.ConversionPattern=%d %p [%c] - %m%n
```
##### 2.4.2.3、代码实现
###### 2.4.2.3.1、map阶段
```java
package com.song.hadoopdemo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map阶段
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
```
###### 2.4.2.3.2、reduce阶段
```java
package com.song.hadoopdemo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    int sum;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2 输出
        v.set(sum);
        context.write(key,v);
    }

}
```

###### 2.4.2.3.3、driver阶段
（1）编写Driver代码
```java
package com.song.hadoopdemo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName WordCountByWindowDriver
 * @Description
 * @Author swq
 * @Date 2022/12/8 17:51
 * @Version 1.0
 */
public class WordCountByWindowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();

        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //指定MR运行在Yarn上
        conf.set("mapreduce.framework.name", "yarn");
        //指定MR可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname","hadoop103");
        Job job = Job.getInstance(conf);
        // 2 设置jar加载路径
       job.setJarByClass(WordCountDriver.class);
        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileInputFormat.setInputPaths(job, new Path("/testRemove/wordCount.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/out"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
```

（2）将编写完的打成jar包，找一个找一个非中文目录存放，并在Driver中重新设置Jar的引用地址

```xml
package com.song.hadoopdemo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName WordCountByWindowDriver
 * @Description
 * @Author swq
 * @Date 2022/12/8 17:51
 * @Version 1.0
 */
public class WordCountByWindowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();

        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //指定MR运行在Yarn上
        conf.set("mapreduce.framework.name", "yarn");
        //指定MR可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname","hadoop103");
        Job job = Job.getInstance(conf);
        // 2 设置jar加载路径
//        job.setJarByClass(WordCountDriver.class);

        job.setJar("C:\\Users\\33229\\Desktop\\jar\\hadoopdemo-0.0.1-SNAPSHOT.jar");
        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileInputFormat.setInputPaths(job, new Path("/testRemove/wordCount.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/out"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
```
（3）设置参数

![在这里插入图片描述](https://img-blog.csdnimg.cn/3987b32c9cb04c7bbc144171f8d0c584.png)
（4）如果使用的是args接受参数，则在原先的基础上添加Program arguments参数配置

```java
 FileInputFormat.setInputPaths(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
```

![在这里插入图片描述](https://img-blog.csdnimg.cn/6b9a52bce78244a28c817c39e7cbb872.png)


