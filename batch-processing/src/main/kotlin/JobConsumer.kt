
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration.*
import java.util.*

class JobConsumer

    fun main (args: Array<String>){
        consumingDataForAnalytics();
    }


    private fun setUpConsumer(groupId: String): KafkaConsumer<String,String> {
        var props = Properties();
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[ConsumerConfig.GROUP_ID_CONFIG] = groupId

        /**********************************************************************
         * Set Batching Parameters
         */

        /**
         *Set batch by number i.e when record count reached 5 then start sending data
         */

//        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,5.toString())

        /**
         * Set bach by size and time
         */
        //Set min bytes to 10 bytes
        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10.toString())

        //Set max wait timeout to 100 ms
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100.toString())

        //Set max fetch size per partition to 1 KB. Note that this will depend on total
        //memory available to the process
        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024.toString())


        /**
         * SET AUTO COMMIT TO FALSE IF YOU WANT TO COMMIT MANUALLY
         * Consumer group will keep on receiving the data copy unless you didn`t inform kafka for each msg
         * that it consumes and process successfully
         */

        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString())

        print("---------------CONSUMER FOR ANALYTICS IS READY-------------")
        return KafkaConsumer(props);
    }

    fun consumingDataForAnalytics() {

        var consumer2 = setUpConsumer("analytics-consumer-group");
        consumer2.subscribe(listOf("sales.new.jobs"))

        while (true){

            var messages = consumer2.poll(ofMillis(2000))
            if(messages.count()>0){
                println("")
                println("----------2nd CONSUMER-------")
                println("BATCH RECEIVED OF SIZE: ${messages.count()}")
            }

            messages.forEach {
                println("Key ${it.key()}")
                println("Value ${it.value()}")
                println("------Partition: ${it.partition()}---- offset: ${it.offset()}-----")

                // Save to Db or move to some other topic for processing
                // you can also implement multithreading here for best performance
            }

            // use this code to commit when ENABLE_AUTO_COMMIT_CONFIG is OFF
            consumer2.commitAsync()

        }
        consumer2.close();
    }
