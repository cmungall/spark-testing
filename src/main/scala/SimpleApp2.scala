import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp2 {
    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
        val sc = new SparkContext(conf)

        val i = Individual(1L, "", Set(1L, 2L, 3L))
        val j = Individual(2L, "", Set(2L, 3L, 4L))
        
        val individuals = Array(i, j)
        val individualsRDD = sc.parallelize(individuals)
        
        var m = new JaccardMatcher()
        val scores = individualsRDD.map { x => m.sim(i, x) }
        
        
        println(s"Scores $scores")
        sc.stop()
    }
}
