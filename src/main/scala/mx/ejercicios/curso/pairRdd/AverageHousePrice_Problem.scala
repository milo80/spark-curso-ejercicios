package mx.ejercicios.curso.pairRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  Elabore el codigo que resuelva las siguientes condiciones:
   Calcule el valor promedio para las siguientes opciones
  1. 3 a 5 recamaras
  2. 4 recamaras con 2 baños
  3. 3 recamaras con 2 baños que tengan status de "Short Sale"

  Imprime en terminal las 3 opciones
 */
object AverageHousePrice_Problem {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    // Complete el codigo

  }
}
