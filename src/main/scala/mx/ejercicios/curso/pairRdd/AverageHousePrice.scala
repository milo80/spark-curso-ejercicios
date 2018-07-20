package mx.ejercicios.curso.pairRdd

import mx.ejercicios.curso.utilities.AvgCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  A partir de los datos de bienes raices del archivo "in/RealEstate.csv"
  Calcule el precio promedio de una casa de acuerdo al numero de habitaciones

  El archivo de datos contiene una tabla con las siguientes columnas:
    1. MLS: (unique ID).
    2. Location: localidad
    3. Price: (en dollars).
    4. Bedrooms: numero de habitaciones.
    5. Bathrooms: numero de baños.
    6. Size: tamaño por pies cuadrados 
    7. Price/SQ.ft: precio por pies cuadrados
    8. Status: hay tres tipos de status: Short Sale, Foreclosure and Regular.
 */
object AverageHousePrice {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    val housePricePairRdd = cleanedLines.map(
      line => (line.split(",")(3).toInt, AvgCount(1, line.split(",")(2).toDouble)))

    val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)

    val sortedHousePriceAvg = housePriceAvg.sortByKey(ascending = false)

   for ((bedrooms, avgPrice) <- sortedHousePriceAvg.collect()) println(bedrooms + " : " + avgPrice)
  }
}
