package com.jyong.spark.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by wangjunyong on 2021/1/7 17:54
 * <p>
 * 利用多线程批量提交Spark任务
 * 注意：任务中视图表的创建、临时表的创建、等共享变量的创建，多个任务同时执行时，会造成同时使用
 */
public class ThreadSpark {


    public static void main(String[] args) {
        String sql1 = "select count(1) from pub_penalty where dt=20210106";
        String sql2 = "select count(1) from pub_penalty_tmp";
        String sql3 = "select count(1) from pub_permission_tmp";
        String sql4 = "select count(1) from pub_permission";
        String sql5 = "select count(1) from test_sort";
        ArrayList<String> list = new ArrayList<>();
        list.add(sql1);
        list.add(sql2);
        list.add(sql3);
        list.add(sql4);
        list.add(sql5);


        SparkSession sparkSession = initSparkSession();



        ThreadSpark.runThreadTask1(sparkSession, list);

    }

    /**
     * 使用Future<String> get阻塞方法进行任务的提交
     * get()是个阻塞方法，会等待这个线程提交的任务执行完毕
     *
     * @param sparkSession
     * @param list
     */

    private static void runThreadTask2(SparkSession sparkSession, List<String> list) {
        //启动多线程
        ExecutorService executorService = Executors.newFixedThreadPool(list.size());
        CountDownLatch latch = new CountDownLatch(list.size());
        for (String s : list) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Dataset<Row> sql = sparkSession.sql(s);
                    sql.show();
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();
        sparkSession.close();
        System.out.println("=====任务执行完毕====");
    }


    private static void runThreadTask1(SparkSession sparkSession, List<String> list) {
        //启动多线程
        ExecutorService executorService = Executors.newFixedThreadPool(list.size());
        ArrayList<Future<String>> list1 = new ArrayList<>();
        for (String s : list) {
            //使用Callable具有返回值的多线程方法提交任务
            Future<String> submit = executorService.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Dataset<Row> sql = sparkSession.sql(s);
                    sql.show();
                    return "success" + s;
                }
            });
            //将任务返回值添加到list集合
            list1.add(submit);
        }
        for (Future<String> result : list1) {
            try {
                //get是一个阻塞方法，获取结果值
                String retult = result.get();
                System.out.println(retult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //关闭session和线程池
        executorService.shutdown();
        sparkSession.close();
        System.out.println("=====任务执行完毕====");
    }

    private static SparkSession initSparkSession() {
        System.setProperty("hadoop.home.dir", "D:\\appinstall");
        System.setProperty("HADOOP_USER_NAME", "bbdoffline");
        SparkConf conf = new SparkConf();
        conf.setAppName("somnambulist-job");
        SparkSession sparkSession = SparkSession.builder().config(conf).master("local[*]").enableHiveSupport().getOrCreate();
        sparkSession.sparkContext().setLogLevel("WARN");
        return sparkSession;
    }
}
