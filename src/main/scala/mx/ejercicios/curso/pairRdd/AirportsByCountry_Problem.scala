package mx.ejercicios.curso.pairRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  Resuelva el siguiente problema utilizando groupByKey()

  Genere un pairRDD  Llave/Valor con las siguientes columnas:
  key : "Country where airport is located"
  value : "Name of airport"

  Posteriormente agrupe todos los aeropuertos por su respectivo país


  El archivo contiene cabeceras y no tiene que procesarlas
  Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

  Imprima resultados en pantalla

  ejemplo:
  "Montenegro": List("Podgorica", "Tivat")
  "Comoros": List("Prince Said Ibrahim", "Bandaressalam", "Ouani", "Iconi Airport")
  "Slovenia": List("Cerklje", "Ljubljana", "Maribor", "Portoroz", "Slovenj Gradec")
 */

object AirportsByCountry_Problem {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/airports.text")
    //
    lines.take(4).foreach(println)


  }

}
