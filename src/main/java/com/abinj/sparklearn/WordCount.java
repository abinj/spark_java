package com.abinj.sparklearn;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("/home/abin/my_works/github_works/sparklearn/src/main/resources/secret.txt");
        JavaPairRDD<String, Integer> counts = input
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a + b);
        System.out.println(counts.count());
    }
}

