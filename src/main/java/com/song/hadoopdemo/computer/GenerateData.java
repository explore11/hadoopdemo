package com.song.hadoopdemo.computer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 测试数据神生成
 */
public class GenerateData {
    private static Random random = new Random();
    private static String[] charts = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
    //循环次数
    private static Long num = 1800000000L;
    //开始区域
    private static Integer start = 1;
    //结束区域
    private static Integer end = 22;


    /**
     * 生成随机索引
     * @param start
     * @param end
     * @return
     */
    public static String generateRandomData(int start, int end) {
        return charts[random.nextInt(end - start + 1)];
    }


    /**
     * 产生10G的 1-1000的数据在D盘
     */
    public void generateData() throws IOException {
        File file = new File("D:\\WordCount.txt");
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        long startTime = System.currentTimeMillis();
        BufferedWriter bos = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
        for (long i = 1; i <= num; i++) {
            String data = "";
            if (i % 2 != 0) {
                data = generateRandomData(start, end) + generateRandomData(start, end) + " ";
            } else {
                data = generateRandomData(start, end) + generateRandomData(start, end);
            }
            bos.write(data);
            // 每100万条记录成一行，100万条数据大概4M
            if (i % 2 == 0) {
                bos.write("\n");
            }
        }
        System.out.println("写入完成! 共花费时间:" + (System.currentTimeMillis() - startTime) / 1000 + " s");
        bos.close();
    }


    public static void main(String[] args) {
        GenerateData generateData = new GenerateData();
        try {
            generateData.generateData();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}