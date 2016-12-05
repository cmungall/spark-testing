import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkRunner {
    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
        val sc = new SparkContext(conf)

        // val i = Individual(1L, "", Set(1L, 2L, 3L))
        //val j = Individual(2L, "", Set(2L, 3L, 4L))
        
        
        //val individuals = Array(i, j)
        val individuals = Util.makeRandomIndividuals(500, 10000, 100)
        val i1 = individuals(1)
        
        val individualsRDD = sc.parallelize(individuals)
        
        var m = new JaccardMatcher()
        val scores = individualsRDD.map { x => m.sim(i1, x) }
        val totalScores = scores.reduce(_+_)
        val avgScore = totalScores / individuals.size
        
        
        println(s"Avg score $avgScore")
        sc.stop()
    }
}
