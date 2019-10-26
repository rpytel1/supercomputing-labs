package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, KeyValue} 
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore, Stores}

import scala.collection.JavaConversions._
import scala.util.matching.Regex

object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // first create the stateStore for counting the number of occurences per record name
  val countStoreSupplier = Stores
    .keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Counts-last-hour"), 
      Serdes.String, 
      Serdes.Long
    )

  // and then create the stateStore for the timestamps
  val timeStoreSupplier = Stores
    .keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Timestamps-last-hour"), 
      Serdes.String, 
      Serdes.Long
    )
  
  builder.addStateStore(countStoreSupplier)
  builder.addStateStore(timeStoreSupplier)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally,
  // write the result to a new topic called gdelt-histogram.
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  records.mapValues(value => value.split("\t"))     // first split the records by tabs
         .filter((_, value) => value.length > 23)   // then keep only those with an AllNames column
         .flatMapValues(value => value(23).replaceAll("[,0-9]", "").split(";"))   // and remove commas and numbers and split by ; to retrieve each record in an article
         .filter((_, value) => !value.trim.equals(""))    // remove all records that after trimming are empty
         .transform[String, Long](() => new HistogramTransformer(), "Counts-last-hour", "Timestamps-last-hour")   // and apply the HistogramTransformer
         .to("gdelt-histogram")  

  val streams: KafkaStreams = new KafkaStreams(builder.build() , props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

}

// This transformer should count the number of times a name occurs
// during the last hour. This means it needs to be able to
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, KeyValue[String, Long]] {
  var context: ProcessorContext = _
  var kvStore: KeyValueStore[String, Long] = _
  var timeStore: KeyValueStore[String, Long] = _

  // Initialize Transformer object
  override def init(context: ProcessorContext): Unit = {
    this.context = context
    // retrieve the key-value store named "Counts-last-hour"
    this.kvStore = context.getStateStore("Counts-last-hour").asInstanceOf[KeyValueStore[String, Long]]
    // retrieve the key-value store named "Timestamps-last-hour"
    this.timeStore = context.getStateStore("Timestamps-last-hour").asInstanceOf[KeyValueStore[String, Long]]

    // initiate the scheduler to be invoked every 500 miliseconds of stream time to check if a record is older than an hour
    this.context.schedule(500, PunctuationType.STREAM_TIME, timestamp => {

      val iterator: KeyValueIterator[String, Long] = timeStore.all()  // take an iterator of the timestamp stateStore
      val retainPeriod: Long = TimeUnit.MILLISECONDS.convert(1L, TimeUnit.HOURS)  // the retaining period for records

      while (iterator.hasNext) {

        val e: KeyValue[String, Long] = iterator.next()   
        val recordTime: Long = e.key.split('|')(0).toLong   // take the record's timestamp
        val record: String = e.key.split('|')(1)            // take the record's name

        // If it more than an hour has passed 
        if (((timestamp - recordTime) / retainPeriod) > 1) {
          kvStore.put(record, kvStore.get(record) - e.value)  // decrement the count of the record by the count of the timestamp-record pair
          timeStore.delete(e.key) // and delete the timestamp-record entry from the timestamp statestore
        }
        // and forward the decreased key-value pair to the downstream processor 
        context.forward(record, kvStore.get(record))
      }
      context.commit()
    })
  }

  // Should return the current count of the name during the _last_ hour
  override def transform(key: String, name: String): KeyValue[String, Long] = {
    // retrieve the timestamp of the record
    val recordTimestamp: Long = this.context.timestamp()
    // and create the key of the timeStore by concatenating the record's timestamp and its name with a pipe in between (for uniqueness)
    val recordTimeKey: String = recordTimestamp.toString + '|' + name

    // add a new record to the count and the timestamp stateStores
    this.kvStore.putIfAbsent(name, 0L)
    this.timeStore.putIfAbsent(recordTimeKey, 0L)

    // increment the record's time-count by 1 (so that we know how many records with the same timestamp and name we had for the needs of the decreasing part)
    val newTimeCount = this.timeStore.get(recordTimeKey) + 1L
    this.timeStore.put(recordTimeKey, newTimeCount)

    // increment the record's count by 1
    val newCount = this.kvStore.get(name) + 1L
    this.kvStore.put(name, newCount)

    // and finally return the updated count of the record 
    (name, newCount)
  }

  // Close any resources if any
  override def close(): Unit = {}
}