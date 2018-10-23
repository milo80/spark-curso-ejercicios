package mx.ejercicios.curso.sumOfNumbers

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/*
  Crea un programa de spark que lea los primeros 100 numeros primos del archivo
  ""Crea un programa de spark que lea los primeros 100 numeros primos del archivo
  "in/prime_nums.txt", imprime la suma de los numeros en la consola

  Cada linea del archivo contiene 10 numeros primos separados por espacios
 */

object SumOfNumbers {
  def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("in/prime_nums.text")
        lines.take(5).foreach(println)
        val numbers = lines.flatMap(line => line.split("\\s+"))
        println("   ----- -  ")
        numbers.foreach(println)
        val validNumbers = numbers.filter(number => !number.isEmpty)
        println("validNumbers es del tipo : "+validNumbers.getClass.getSimpleName)
        val intNumbers = validNumbers.map(number => number.toInt)

        println("Sum is: " + intNumbers.reduce((x, y) => x + y))
    }

}
