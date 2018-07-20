package mx.ejercicios.curso.rdd

import org.apache.spark.{SparkConf, SparkContext}
import mx.ejercicios.curso.utilities.Utils

/*
  Crea el codigo para leer los datos del archivo "in/airports.text"
  Cree un nuevo RDD con todos los aeropuertod que esten en latitud
  mayor a 40 [deg]
  Los resultados guardalos en un archivo "output/airports_by_latitud.txt"

  Cada elemento de los renglones corresponde  las siguientes numbres de columnas
   Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 */

object AirportByLatitude {
 def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportsNameAndCityNames.saveAsTextFile("output/airports_by_latitude.txt")
  }
}
