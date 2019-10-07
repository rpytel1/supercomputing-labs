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

        val spark = SparkSession.builder.appName("SBD_Lab1").getOrCreate()

        import spark.implicits._	
		
        //the final JSON schema 
        val finalJSONSchema: String = "array<struct<topic:string,count:bigint>>"

        val ds = spark.read.format("csv")
                .option("delimiter", "\t")              // set the delimeter option as tab
                .option("dateFormat", "yyyyMMddHHmmss") // set the date format
                .schema(schema)
                .csv(args(0))
                .as[GdeltData]

        val processed_ds = ds
                            .filter(col("DATE").isNotNull && col("AllNames").isNotNull)                         // filter out the null entries
                            .select("DATE", "AllNames")                                                         // keep only the DATE and AllNames columns
                            .select($"DATE", explode(array_distinct(split(regexp_replace($"AllNames" ,"[,0-9]", ""), ";"))).as("AllNames"))  // clean the entity names -> convert them into set -> create date-name pairs
                            .filter(!col("AllNames").contains("Type ParentCategory"))                           // Filter out ParentCategory
                            .groupBy("DATE", "AllNames")                                                        // group by the columns in order to count the occurences
                            .count                                                                              // of each distinct name in each day
                            .withColumn("Rank", rank.over(Window.partitionBy("DATE").orderBy($"count".desc)))   // partition by date and find the rank in each day window
                            .filter(col("Rank") <= 10)                                                          // keep only the top 10 counts for each day
                            .groupBy("DATE")                                                                    // group by DATE
                            .agg(collect_list(struct("AllNames", "count")).as("TopNames"))					    // and collect top-10 name-count pairs as a list on each date 
                            .orderBy($"DATE".asc)                                                               // and ascendingly order by date
                            .select('DATE as "data", 'TopNames.cast(finalJSONSchema) as "result")               // and apply the final JSON format
                            .toJSON

        processed_ds
                    .coalesce(1)        // can be commented out for parallel S3 writes
                    .write
                    .text(args(1))      // write the wanted result

        spark.stop()
    }
}
