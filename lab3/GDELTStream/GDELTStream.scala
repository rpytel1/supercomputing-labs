package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, KeyValue, Topology} // to check topology
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

  val countStoreSupplier = Stores
    .keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Counts-last-hour"), 
      Serdes.String, 
      Serdes.Long
    )
    // .withLoggingDisabled()

  val timeStoreSupplier = Stores
    .keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Timestamps-last-hour"), 
      Serdes.String, 
      Serdes.String
    )
  
  builder.addStateStore(countStoreSupplier)
  builder.addStateStore(timeStoreSupplier)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally,
  // write the result to a new topic called gdelt-histogram.
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  // val wordPattern = "\\p{L}".r

  records.mapValues(value => value.split("\t"))
         .filter((_, value) => value.length > 23)
         .flatMapValues(value => value(23).replaceAll("[,0-9]", "").split(";"))
         .mapValues(value => value.trim)
         // .mapValues(value => value.replaceAll("\n", "").replaceAll("""(?m)\s+$""", "").trim) // maybe to be removed
         .filter((_, value) => !value.trim.equals(""))
         // .filter((_, value) => wordPattern.findFirstIn(value)!= None)
         // .filter((_, value) => wordPattern.findFirstIn(value)!= None && !value.trim.equals("\\P{Other}") && !value.trim.equals("\\P{Z}"))
         .transform[String, Long](() => new HistogramTransformer(), "Counts-last-hour", "Timestamps-last-hour")
         .to("gdelt-histogram")

  
  // val topology: Topology = builder.build()  // just to check topology
  // println(topology.describe)                // the created topology
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
  var timeStore: KeyValueStore[String, String] = _

  // Initialize Transformer object
  override def init(context: ProcessorContext): Unit = {
    this.context = context
    // retrieve the key-value store named "Counts-last-hour"
    this.kvStore = context.getStateStore("Counts-last-hour").asInstanceOf[KeyValueStore[String, Long]]
    this.timeStore = context.getStateStore("Timestamps-last-hour").asInstanceOf[KeyValueStore[String, String]]

    this.context.schedule(500, PunctuationType.STREAM_TIME, timestamp => {

      val iterator: KeyValueIterator[String, String] = timeStore.all()

      while (iterator.hasNext) {

        val e: KeyValue[String, String] = iterator.next()

        // If it was more than an hour has passed
        if (((timestamp - e.key.toLong) / 20000) > 1) {
          //3. Decrement the count of a record an hour later.
          kvStore.put(e.value, kvStore.get(e.value) - 1L)
          //Delete it from the log
          timeStore.delete(e.key)
        }
        //Output of a name with decremented count based on time
        context.forward(e.value, kvStore.get(e.value))
      }
      context.commit()
    })
  }

  // Should return the current count of the name during the _last_ hour
  override def transform(key: String, name: String): KeyValue[String, Long] = {
    // set the interval to 1 hour (for testing reasons it is set to 20 secs)
    // val interval: Long = TimeUnit.MILLISECONDS.convert(20L, TimeUnit.SECONDS)
    val recordTimestamp: Long = this.context.timestamp()

    // schedule the decrementing of the count after 1 hour
    // var scheduledDecrease: Cancellable = null

    // // PunctuationType.STREAM_TIME -> for taking into account the attached timestamps 
    // // PunctuationType.WALL_CLOCK_TIME -> for taking into account the real clock
    // scheduledDecrease = this.context.schedule(interval, PunctuationType.STREAM_TIME, timestamp => {

    //   // decrement by 1
    //   val newCount = kvStore.get(name) - 1L
    //   kvStore.put(name, newCount)
    //   context.forward(name, newCount)

    //   // cancel this schedule, so it runs only once
    //   scheduledDecrease.cancel()
    // })
    // add new record to StateStore
    this.kvStore.putIfAbsent(name, 0L)

    this.timeStore.put(recordTimestamp.toString, name)

    // increment the record's count by 1
    val newCount = this.kvStore.get(name) + 1L
    this.kvStore.put(name, newCount)

    // and finally return the count of the record over the last hour
    (name, newCount)
  }

  // Close any resources if any
  override def close(): Unit = {}
}