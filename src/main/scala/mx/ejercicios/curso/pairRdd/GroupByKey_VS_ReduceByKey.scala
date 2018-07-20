package mx.ejercicios.curso.pairRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  Ejemplo : como resolver WordCount
  reduceBykey contra groupByKey
 */

object GroupByKey_VS_ReduceByKey {
   def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = List("one", "two", "two", "three", "three", "three")
    val wordsPairRdd = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) => x + y).collect()
    println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey.toList)

    val wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(intIterable => intIterable.size).collect()
    println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey.toList)
  }

}
