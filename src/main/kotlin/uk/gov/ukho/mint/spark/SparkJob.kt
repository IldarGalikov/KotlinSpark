package uk.gov.ukho.mint.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession

abstract class SparkJob(
        private val name: String,
        private val master: String = "local[*]"
) {
    fun exec() {
        val sc = SparkContext.getOrCreate(
                SparkConf()
                        .setAppName(name)
                        .setMaster(master)
        )
        val jsc = JavaSparkContext.fromSparkContext(sc)
        run(jsc)
        sc.stop()
    }

    protected abstract fun run(jsc: JavaSparkContext)
}
