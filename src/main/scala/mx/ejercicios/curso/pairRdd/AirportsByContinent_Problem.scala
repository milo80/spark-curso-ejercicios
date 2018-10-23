package mx.ejercicios.curso.pairRdd

//import mx.ejercicios.curso.utilities.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
/*
  Resuelva el siguiente problema utilizando filter sobre un pairRDD

  Genere un pairRDD con:
  key : Pais al que pertenece el aeropuerto
  Value : nombre del aeropuerto

  Imprima el RDD con los aeropuertos que pertenecen al continente americano, Africano, Indoeuropeo
  Nota : cree una lista con los nombres de paises agrupado por continente

  Guarde los resultados en un archivo de texto "output/AirportsByContinent.txt"

 */

object AirportsByContinent_Problem {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airports").setMaster("local")
    val sc = new SparkContext(conf)

    val airportsRDD = sc.textFile("in/AirportsByContinent.text")
    //
    airportsRDD.take(10).foreach(println)



  }
}
