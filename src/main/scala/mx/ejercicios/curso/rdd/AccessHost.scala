package mx.ejercicios.curso.rdd

import org.apache.spark.{SparkConf, SparkContext}

/*
   El archivo "in/nasa_19950701.tsv" contiene 1000 lineas de registro de
  de un servidor apache para el 1ro de julio de 1995
  El archivo "in/nasa_19950801.tsv" contiene 1000 lineas de registro de
  de un servidor apache para el 1ro de Agosto de 1995

  Crea un programa spark que genere un RDD que contenga el host que fue
  accesado en ambos dias.

  Guarde el resultado en el archivo "output/nasa_logs_same_host.csv"
 */
object AccessHost {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val julyFirstHosts = julyFirstLogs.map(line => line.split("\t")(0))
    val augustFirstHosts = augustFirstLogs.map(line => line.split("\t")(0))

    val intersection = julyFirstHosts.intersection(augustFirstHosts)

    val cleanedHostIntersection = intersection.filter(host => host != "host")
    cleanedHostIntersection.saveAsTextFile("output/nasa_logs_same_hosts.csv")
  }
}
