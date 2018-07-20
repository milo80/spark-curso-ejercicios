package mx.ejercicios.curso.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}

/*
  Extraiga las dos primeras columnas de la tabla "in/maxPrice.txt"
  Haga una operacion que calcule el máximo precio por año de la tabla
  del historial financiero
 */
object MaxPrice {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Max Price").setMaster("local")
    val sc = new SparkContext(conf)


    sc.textFile("in/tableHistorical.csv")
      .map(_.split(","))
      .map(rec => ((rec(0).split("-"))(0).toInt, rec(1).toFloat))
      .reduceByKey((a,b) => Math.max(a,b))
      .saveAsTextFile("output/maxPrice.txt")

    println("MaxPrice se ha ejecutado exitosamente")
    // Nota : para ejecutar nuevamente borre el archivo de salida
    // o cambia de nombre de archivo "output/maxPrice_0.txt")

  }

}
