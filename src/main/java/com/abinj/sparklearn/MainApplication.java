package com.abinj.sparklearn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

public class MainApplication {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("main");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> input = sc.textFile("/home/abin/my_works/github_works/sparklearn/src/main/resources/secret.txt");
//        JavaRDD<String> filteredOut = input.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return (s.contains("Reshma")|s.contains("Akhil"));
//            }
//        });
//        System.out.println(filteredOut.collect());




        JavaRDD<String> lines = sc.parallelize(Arrays.asList("Hello World", "Hi"));
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println(words.first());
    }
}
