import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.WindowStore
import java.sql.Timestamp
import java.time.Duration.*
import java.util.*
import java.util.concurrent.CountDownLatch

class ProductAnalyticStream

    var builder = StreamsBuilder();
    var props = Properties()


    fun main() {
        streamListener()
    }

    fun setUpStream(): KStream<String, String>? {
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "products-click-analytic-stream"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.StringSerde().javaClass
        return builder.stream("product.click.events")
    }

    fun streamListener() {
       var stream = setUpStream()
       println("--------STREAM CONFIGURED--------")
       var eventStream = stream!!
//           .peek { k, v -> println("KEY-----$k------- VALUE ----- $v") }
           .mapValues { v -> mapToDto(v) }


        /**
         *  Aggregator Needs initializer and aggregator function (k,v,agre)
         *
         *  def: aggregate(
         *  param 1: Initializer<VR> var1,
         *  param 2: Aggregator<
         *  ? super K,   // key
         *  ? super V,   // vlaue
         *  VR> var2,    // agre
         *  Materialized<K, VR, KeyValueStore<Bytes, byte[]>> var3);
         */


        //Initialize on each new Batch received after 20 sec
        val viewAggregatorInitializer: Initializer<AnalyticViewAggregator> =
            Initializer { AnalyticViewAggregator() }

        //Aggregate using the view aggregator.
        val viewAggr =
            Aggregator<String?, AnalyticDto?, AnalyticViewAggregator> { _, _, aggregate: AnalyticViewAggregator ->
                aggregate.add(1)
            }


        //Create a time window of 5 seconds
        val tumblingWindow = TimeWindows.ofSizeWithNoGrace(ofSeconds(20))

        val aggregatorSerde = Serdes.serdeFrom(
            GenericSerializer(),
            GenericDeserializer(AnalyticViewAggregator::class.java)
        )



        println("--------PROCESSING EVENT STREAM DATA--------")

        var kTable: KTable<Windowed<String?>, AnalyticViewAggregator>? =  eventStream!!
//            .peek { k, v -> println("KEY-----$k------- VALUE PEEK ----- ${v?.productName}") }
            .groupByKey()
            .windowedBy(tumblingWindow)
            .aggregate(
                viewAggregatorInitializer, // initializer
                viewAggr, // key,value,aggregator to be stored
                Materialized.`as`<String, AnalyticViewAggregator, WindowStore<Bytes, ByteArray>>(
                    "time-windowed-aggregate-store"
                )
                    .withValueSerde(aggregatorSerde)
            )


        /**
         *  SUMMARY OF MOST VISITED PRODUCTS IN 5 SEC
         */
        println("--------KTABLE SUMMARY--------")


        /**************************************************
         * Create a pipe and execute
         **************************************************/

        val topology = builder.build()
        val kafkaStream = KafkaStreams(topology,props)
        kafkaStream.cleanUp()

        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-shutdown-hook") {
            override fun run() {
                println("Shutdown called..")
                kafkaStream.close()
                latch.countDown()
            }
        })

        kafkaStream.start()
        latch.await()

    }

    fun mapToDto(inputCSV: String): AnalyticDto? {
        var arr = inputCSV.split(",");

        if(arr.size>2) {
            return AnalyticDto(arr[0],arr[1],arr[2])
        }
        return AnalyticDto("0","","CLICK")

    }
