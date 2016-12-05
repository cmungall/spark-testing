/* SimpleApp2.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.math.sqrt

case class Individual(id: Long, label: String, inferredTypes: Set[Long])

trait Matcher extends java.io.Serializable {
  def sim(i: Individual, j: Individual) : Double
}

class JaccardMatcher extends Matcher {

  def sim(i: Individual, j: Individual) : Double = {
    val ca = i.inferredTypes.intersect(j.inferredTypes)
    val cu = i.inferredTypes.union(j.inferredTypes)
    return ca.size / cu.size.toDouble
  }
}

class CosineMatcher extends Matcher {

  def sim(i: Individual, j: Individual) : Double = {
    val sumOfVectorProduct = i.inferredTypes.intersect(j.inferredTypes).size.toDouble
    return sumOfVectorProduct / (sqrt(i.inferredTypes.size) * sqrt(i.inferredTypes.size))
  }
}

object Util {
  def makeRandomIndividuals(numClasses: Integer, numIndividuals: Integer,
      numTypesPerIndividual: Integer) : Seq[Individual] = {
    val inds = 1 to numIndividuals map { 
      x => makeRandomIndividual(x, numClasses, numTypesPerIndividual) 
    }
    return inds
  }
  def makeRandomIndividual(id: Long, numClasses: Integer, numTypesPerIndividual: Integer) : Individual = {
    var r = new scala.util.Random
    val rtypes = 1 to numTypesPerIndividual map { _ => r.nextInt(100).toLong }
    return Individual(id=id, label="", inferredTypes=rtypes.toSet)
  }
}

object JaccardTest {
  def main(args: Array[String]) {
    val i = Individual(1L, "", Set(1L, 2L, 3L))
    val j = Individual(2L, "", Set(2L, 3L, 4L))
    var m = new JaccardMatcher()
    
    println(s"FOO" + m.sim(i,j))
  }
}
