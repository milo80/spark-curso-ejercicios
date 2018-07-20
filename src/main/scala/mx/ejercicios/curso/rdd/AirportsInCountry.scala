package mx.ejercicios.curso.rdd

import mx.ejercicios.curso.utilities.Utils
import org.apache.spark.{SparkConf, SparkContext}

/*
  Crea el codigo para leer los datos del archivo "in/airports.text"
  Cree un nuevo RDD con todos los aeropuertod de Canada y guardelos en
  el archivo "output/airports_in_canada.txt"

  el archivo de salida solo debe de contener "Name of airport", "Main City .." "Country ..."

  Considere que el archivo contiene cabeceras y no tiene que procesarlas
  Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 */

object AirportsInCountry {
def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"Canada\"")

    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2) + ", " + splits(3)
    })
    airportsNameAndCityNames.saveAsTextFile("output/airports_in_canada.text")
  }
}
