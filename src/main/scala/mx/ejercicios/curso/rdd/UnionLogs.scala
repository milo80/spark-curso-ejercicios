package mx.ejercicios.curso.rdd

import org.apache.spark.{SparkConf, SparkContext}
/*
  El archivo "in/nasa_19950701.tsv" contiene 1000 lineas de registro de
  de un servidor apache para el 1ro de julio de 1995
  El archivo "in/nasa_19950801.tsv" contiene 1000 lineas de registro de
  de un servidor apache para el 1ro de Agosto de 1995

  Crea un programa de spark para generar un nuevo RDD que contiene las lineas
  de registro de ambos archivos (julio y agosto).

  Tome una muestra del 10% de esas lineas de registro y almacenalas en
  "output/sample_nasa_logs.tsv"
  Tome en cuenta que los registros tienen texto de las cabeceras. Asegurese
  de que no utilizar la primer linea de cabeceras
*/

object UnionLogs {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

    val cleanLogLines = aggregatedLogLines.filter(line => isNotHeader(line))

    val sample = cleanLogLines.sample(withReplacement = true, fraction = 0.1)

    sample.saveAsTextFile("output/sample_nasa_logs.csv")

    println(" Unilogs, ejecutado correctamente ... ")
    // Nota : para ejecutar nuevamente se tiene que borrar el archivo creado
    // "output/sample_nasa_logs.csv") ... toda la carpeta
    // Por default no se puede sobreescribir un archivo, ésto con fines de seguridad de los datos.

    // Para ver resultados en consola ejecutar: (directorio principal del proyecto)
    // cat /output/sample_nasa_logs/part-*
  }

  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))

}
