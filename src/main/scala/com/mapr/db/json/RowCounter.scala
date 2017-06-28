package com.mapr.db.json

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._

object RowCounter {

  def main(args: Array[String]): Unit = {
    val startTotal=System.currentTimeMillis();

    val spark = new SparkConf().setAppName("MapR-DB JSON Row Counter");
    val sc = new SparkContext(spark);

    val allRows = sc.loadFromMapRDB(args(0)).select("_id");


    val start=System.currentTimeMillis();
    print("Number of documents in "+ args(0) +": "+  allRows.count() );
    val end = System.currentTimeMillis();
    print(" (in "+ (end-start) + " ms)"  +"\n");
    print("\t (in "+ (end-startTotal) + " ms with Spark pre-processing work)"  +"\n\n");

  }

}
