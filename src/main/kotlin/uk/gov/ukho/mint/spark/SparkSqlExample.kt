package uk.gov.ukho.mint.spark

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import java.sql.Time


fun main(args: Array<String>) {
    SparkSqlExample("abbbbc").exec()
}

class SparkSqlExample(name: String) : SparkJob(name) {


    private val entropySize = 1_000_000

    private val lowercase = ('a'..'z')
    private val uppercase = ('A'..'Z')
    private val number = ('0'..'9')
    private val symbol = "!@#$%^&^&*(*)".toCharArray()

    override fun run(sc: JavaSparkContext) {
        val ss = SparkSession.builder().sparkContext(sc.sc()).orCreate

        val df = ss.read().csv("src/main/resources/Stuff.csv")
        df.show()
        val timeEntries = sc
                .textFile("src/main/resources/Stuff.csv")
                .map { it.split (",") }
                .map {
                    row -> TimeEntry(row[0], row[12])
                }

//        val timeEntriesDF = ss.createDataFrame(timeEntries, TimeEntry::class)

    }
}

data class TimeEntry(val date: String, val time: String)







