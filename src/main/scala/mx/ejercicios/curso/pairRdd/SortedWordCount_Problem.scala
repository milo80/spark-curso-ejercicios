package mx.ejercicios.curso.pairRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  Completa el codigo para ordenar numéricamente los resultados
  de el pairRDD (word, count)
 */

object SortedWordCount_Problem {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" "))

    val wordPairRdd = wordRdd.map(word => (word, 1))
    val wordToCountPairs = wordPairRdd.reduceByKey((x, y) => x + y)

    val countToWordParis = wordToCountPairs.map(wordToCount => (wordToCount._2, wordToCount._1))

    // Imprime resultado
    for ((word, count) <- countToWordParis.collect()) println(word + " : " + count)

    // Utilizando sortByKey ordena  de mayor a menor los valores de count

    // Imprime nuevamente los resultados




  }
}
