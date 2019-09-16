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
    case class GdeltData (                  // the class of the Gdelt dataset
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

        val schema = StructType(              // the schema of the Gdelt dataset
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

        // a user-defined function for converting a WrappedArray column to Set and then back to Array
        val mkSet = udf((arrayCol: Seq[String]) => arrayCol.asInstanceOf[WrappedArray[String]].toSet.toArray)

        // a user-defined function for converting a list of lists to an array of struct of tuples
        val mkList = udf((arrayCol: Seq[Seq[String]]) => 
            arrayCol.asInstanceOf[WrappedArray[WrappedArray[String]]].map(s => s.toString.replaceAll("[()]", "").replaceAll("\\bWrappedArray","").split(",\\s")).map { case Array(f1,f2) => (f1,f2.toInt)}) 
		
        //the final JSON schema 
        val finalJSONSchema: String = "array<struct<topic:string,count:bigint>>"

        val ds = spark.read.format("csv")
                .option("delimiter", "\t")              // set the delimeter option as tab
                .option("dateFormat", "yyyyMMddHHmmss") // set the date format
                .schema(schema)
                // .csv("data/seg100.csv")
                .csv("data/segment/*.csv")
                .as[GdeltData]

        val processed_ds = ds
                            .filter(col("DATE").isNotNull && col("AllNames").isNotNull)                         // filter out the null entries
                            .select("DATE", "AllNames")                                                         // keep only the DATE and AllNames columns
                            .select($"DATE", explode(mkSet(split(regexp_replace($"AllNames" ,"[,0-9]", ""), ";"))).as("AllNames"))  // clean the entity names -> convert them into set -> create date-name pairs
                            .filter(!col("AllNames").contains("Type ParentCategory"))                           // Filter out ParentCategory
                            .groupBy("DATE", "AllNames")                                                        // group by the columns in order to count the occurences
                            .count                                                                              // of each distinct name in each day
                            .withColumn("Rank", rank.over(Window.partitionBy("DATE").orderBy($"count".desc)))   // partition by date and find the rank in each day window
                            .filter(col("Rank") <= 10)                                                          // keep only the top 10 counts for each day
                            .groupBy("DATE")                                                                    // group by DATE
                            .agg(collect_list(array("AllNames", "count")).as("TopNames"))					    // and collect top-10 name-count pairs as a list on each date 
                            .orderBy($"DATE".asc)                                                               // and ascendingly order by date
                            .withColumn("TopNames", mkList($"TopNames"))                                        // change the structure of the final dataset
                            .select('DATE as "data", 'TopNames.cast(finalJSONSchema) as "result")               // and apply the final JSON format
                            .toJSON
                            // .collect()

        // processed_ds.foreach(println)   // print the wanted result
        processed_ds.coalesce(1)
                    .write
                    .text("data/results/res150")

        spark.stop()
    }


    def time[R](block: => R): R = {
        
        val t0 = System.currentTimeMillis()
        val result = block    // call-by-name
        val t1 = System.currentTimeMillis()
        println("Elapsed time: " + (t1 - t0) + "ms")
        result
    }
}
