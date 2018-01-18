package demo

//package com.huxiaoqiang

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("Word Count").
      setJars(List("/home/ubuntu/IdeaProjects/WordCount/out/artifacts/WordCount_jar/WordCount.jar"))
    val sc = new SparkContext(conf)
    val input = "hdfs://192.168.3.200:9000/spark/Demo/input.txt"
    val output = "hdfs://192.168.3.200:9000/spark/Demo/out"

    val textFile = sc.textFile(input)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(output)
  }
}