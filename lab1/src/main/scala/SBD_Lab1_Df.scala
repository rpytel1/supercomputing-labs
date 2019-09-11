package lab1

import org.apache.spark.sql._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.log4j.{Level, Logger}
import java.sql.Date
import scala.collection.mutable.WrappedArray


object SBD_Lab1_Df {
    case class GdeltData (
	    GKGRECORDID: String,
	    DATE: Date,
	    SourceCollectionIdentifier: Int,
	    SourceCommonName: String,
	    DocumentIdentifier: String,
	    Counts: String,
	    V2Counts: String,
	    Themes: String,
	    V2Themes: String,
	    Locations: String,
	    V2Locations: String,
	    Persons: String,
	    V2Persons: String,
	    Organizations: String,
	    V2Organizations: String,
	    V2Tone: String,
	    Dates: String,
	    GCAM: String,
	    SharingImage: String,
	    RelatedImages: String,
	    SocialImageEmbeds: String,
	    SocialVideoEmbeds: String,
	    Quotations: String,
	    AllNames: String,
	    Amounts: String,
	    TranslationInfo: String,
	    Extras: String
	)

	def main(args: Array[String]) {

		val schema = StructType( 
			Array(
			    StructField("GKGRECORDID", StringType, nullable = true),
			    StructField("DATE", DateType, nullable = true),
			    StructField("SourceCollectionIdentifier", IntegerType, nullable = true),
			    StructField("SourceCommonName", StringType, nullable = true),
			    StructField("DocumentIdentifier", StringType, nullable = true),
			    StructField("Counts", StringType, nullable = true),
			    StructField("V2Counts", StringType, nullable = true),
			    StructField("Themes", StringType, nullable = true),
			    StructField("V2Themes", StringType, nullable = true),
			    StructField("Locations",StringType, nullable = true),
			    StructField("V2Locations",StringType, nullable = true),
			    StructField("Persons", StringType, nullable = true),
			    StructField("V2Persons", StringType, nullable = true),
			    StructField("Organizations", StringType, nullable = true),
			    StructField("V2Organizations", StringType, nullable = true),
			    StructField("V2Tone", StringType, nullable = true),
			    StructField("Dates",StringType, nullable = true),
			    StructField("GCAM", StringType, nullable = true),
			    StructField("SharingImage", StringType, nullable = true),
			    StructField("RelatedImages", StringType, nullable = true),
			    StructField("SocialImageEmbeds", StringType, nullable = true),
			    StructField("SocialVideoEmbeds", StringType, nullable = true),
			    StructField("Quotations", StringType, nullable = true),
			    StructField("AllNames", StringType, nullable = true),
			    StructField("Amounts", StringType, nullable = true),
			    StructField("TranslationInfo", StringType, nullable = true),
			    StructField("Extras", StringType, nullable = true)
			)
		)

		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		
		val spark = SparkSession.builder.appName("SBD_Lab1").config("spark.master", "local[*]").getOrCreate()

		import spark.implicits._	
		
		val mkSet = udf((arrayCol: Seq[String]) => arrayCol.asInstanceOf[WrappedArray[String]].toSet.toArray)

		val ds = spark.read.format("csv").option("delimiter", "\t").option("dateFormat", "yyyyMMddHHmmss").schema(schema).csv("data/seg10.csv").as[GdeltData]

		val processed_ds = ds
						.filter(col("DATE").isNotNull && col("AllNames").isNotNull)
						.select("DATE", "AllNames")
						.withColumn("AllNames", regexp_replace($"AllNames" ,"[,0-9]", ""))
						.withColumn("OnlyNames", split($"AllNames", ";"))
						.drop("AllNames")
						.withColumn("DistinctNames", mkSet($"OnlyNames")) 
						.drop("OnlyNames")
						.withColumn("DistinctNames", explode($"DistinctNames"))
						.groupBy("DATE", "DistinctNames")
						.count
						.withColumn("Rank", rank.over(Window.partitionBy("DATE").orderBy($"count".desc)))
						.filter(col("Rank") <= 10)
						.drop("Rank")
						.withColumn("merged", array("DistinctNames", "count"))
						.drop("DistinctNames")
						.drop("count")
						.groupBy("DATE")
						.agg(collect_list(col("merged")).as("final"))

		processed_ds.take(20).foreach(println)
		
		spark.stop()
	}
}
