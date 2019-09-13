import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext,SparkConf}

object RDD {

case class DateResult (                  // the class of the final format
        Date: String,
        Topics: List[(String,Int)]
    )

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val spark = new SparkContext(conf)

    val rdd = spark.textFile("data/segment")// Extract data from folder

    val processedRDD =  time{rdd.map(_.split("\t")) // Split it by the tabs
      .filter(_.length > 23)// Ommit elements which does not have AllName
      .map(a=>(reformatDate(a(1)),reformatName(a(23))))// Reformat data to only have nicely formatted date and name only
      .flatMapValues(x=>x) // Flatten it to to the key value pair
      .filter(!_._2.contains("Type ParentCategory"))// Filter topics for parents only
      .map(k => (k, 1))// Add 1 to each key
      .reduceByKey(_ + _)// Reduce to get popularity for each date+title instance
      .map(k=>(k._1._1,(k._1._2,k._2)))// Change Structure to have date and name seperetly
      .groupByKey() // Group to have all names with popularity for one date
      .map(g => (g._1, g._2.toList.sortWith(_._2 > _._2).take(10)))}// Order them in decreasing order and take only 10 most popular
      

    processedRDD.foreach(a => println(DateResult(a._1, a._2)))// Print it in the right way


    spark.stop()
  }

  def reformatDate(elem: String): String = {
     elem.take(4) + "-" + elem(4) + elem(5) + "-" +elem (6) + elem(7)
  }

  def reformatName(elem: String): Array[String] = {
  elem.replaceAll("[,0-9]","").split(";").toSet.toArray
  }

  def time[R](block: => R): R = {
        
        val t0 = System.currentTimeMillis()
        val result = block    // call-by-name
        val t1 = System.currentTimeMillis()
        println("Elapsed time: " + (t1 - t0) + "ms")
        result
    }
}
