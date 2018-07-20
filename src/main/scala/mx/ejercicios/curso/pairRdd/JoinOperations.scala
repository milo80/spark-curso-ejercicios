package mx.ejercicios.curso.pairRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  Ejemplos de operaciones join con pairRDDs
 */

object JoinOperations {
   def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf().setAppName("JoinOperations").setMaster("local")
        val sc = new SparkContext(conf)

        val ages = sc.parallelize(List(("Tom", 29),("John", 22)))
        val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))
        println(" Join :")
        val join = ages.join(addresses)
        join.collect().foreach(println)

        println("\n leftOuterJoin :")
        val leftOuterJoin = ages.leftOuterJoin(addresses)
        leftOuterJoin.collect().foreach(println)

        println("\n rightOuterJoin : ")
        val rightOuterJoin = ages.rightOuterJoin(addresses)
        rightOuterJoin.collect().foreach(println)

        println(s"\n fullOuterJoin : ")
        val fullOuterJoin = ages.fullOuterJoin(addresses)
        fullOuterJoin.collect().foreach(println)
    }

}
