package uk.gov.ukho.mint.spark

import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2
import java.security.SecureRandom
import kotlin.coroutines.experimental.buildSequence

class JavaScriptGuyJob(name: String) : SparkJob(name) {


    private val entropySize = 1_000_000

    private val lowercase = ('a'..'z')
    private val uppercase = ('A'..'Z')
    private val number = ('0'..'9')
    private val symbol = "!@#$%^&^&*(*)".toCharArray()

    override fun run(sc: JavaSparkContext) {
        val generator = buildSequence {
            val symbols = lowercase + uppercase + number

            val rand = SecureRandom()
            while (true) {
                yield(
                        (0 until rand.nextInt(16)).joinToString(" ") {
                            (0 until rand.nextInt(8))
                                    .map { rand.nextInt(symbols.size) }
                                    .map { symbols[it] }
                                    .joinToString("")
                        }
                )
            }
        }


        val data = sc.parallelize(generator.take(entropySize).toList(), 16)

        val spaceSeparatedList = data
                .flatMap { it.split(" ").iterator() }
        val aggregatedWordCount = spaceSeparatedList
//                .count
//                .keyBy { it }
//                .mapValues { 1 }
                .mapToPair { Tuple2(it, 1) }
                .reduceByKey { x, y -> x + y }

        aggregatedWordCount
                .filter { it._2 > 5  && it._1.length > 2}
                .toLocalIterator()
                .asSequence()
                .sortedBy { it._2 }
                .forEach { println(it) }

    }
}








