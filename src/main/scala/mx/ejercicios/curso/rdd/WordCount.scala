package mx.ejercicios.curso.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD

/*
  Ejemplo de WordCount en Spark
*/

object WordCount {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts) println(word + " : " + count)
  }

}
