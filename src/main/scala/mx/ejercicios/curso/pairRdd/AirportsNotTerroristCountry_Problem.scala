package mx.ejercicios.curso.pairRdd

//import mx.ejercicios.curso.utilities.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
/*
  Resuelva el siguiente problema utilizando filter sobre un pairRDD

  Genere un pairRDD con:
  key : Pais al que pertenece el aeropuerto
  Value : nombre del aeropuerto

  Imprima el RDD con los aeropuertos de NO pertenecen a paises terroristas
  Paises terroristas son : "United Stated, "Israel", "United Kingdom", "France", "Saudi Arabia"

  Guarde los resultados en un archivo de texto "output/AirportsNotInTerroristSoil.txt"

 */

object AirportsNotTerroristCountry_Problem {
def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airports").setMaster("local")
    val sc = new SparkContext(conf)

    val airportsRDD = sc.textFile("in/airports.text")
    //
    airportsRDD.take(10).foreach(println)



  }
}
