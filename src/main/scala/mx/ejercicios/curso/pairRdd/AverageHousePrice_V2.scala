package mx.ejercicios.curso.pairRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
   A partir de los datos de bienes raices del archivo "in/RealEstate.csv"
  Calcule el precio promedio de una casa de acuerdo al numero de habitaciones

  Version : Utilizando combineBykey()
 */
object AverageHousePrice_V2 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    val housePricePairRdd = cleanedLines.map(line => (line.split(",")(3), line.split(",")(2).toDouble))

    val createCombiner = (x: Double) => (1, x)
    val mergeValue = (avgCount: AvgCount, x: Double) => (avgCount._1 + 1, avgCount._2 + x)
    val mergeCombiners = (avgCountA: AvgCount, avgCountB: AvgCount) => (avgCountA._1 + avgCountB._1, avgCountA._2 + avgCountB._2)

    val housePriceTotal = housePricePairRdd.combineByKey(createCombiner, mergeValue, mergeCombiners)

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount._2 / avgCount._1)
    for ((bedrooms, avgPrice) <- housePriceAvg.collect()) println(bedrooms + " : " + avgPrice)
  }

  type AvgCount = (Int, Double)
}
