import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class JobProducer

    private const val TOPIC_NAME = "sales.new.jobs"

    private lateinit var producer : Producer<String, String>;

    var jobs = listOf<String>("I need to repair my PC","Need a car ride to office","Need to install air conditioner.")

    fun main(args: Array<String>) {

        setUpProducer();

        generateJobs();
    }


    fun setUpProducer() {
        var props = Properties()

        // List of brokers available, if you list one kafka will discover other itself
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

        // Simple SerDes for String Key and Value
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

        producer = KafkaProducer(props)

    }



    fun generateJobs() {
        println("-----------GENERATING DATA------------")

        for (i in 1..50){
            var key = Random().nextInt(1000).toString()
            var value = jobs[Random().nextInt(jobs.size)]

            var record = ProducerRecord(TOPIC_NAME,key,value)
            println("")
            println("-----------SENDING DATA TO PRODUCER------------")
            println("$record")
            producer?.send(record)

            Thread.sleep(2000)
            println("-----------MESSAGE SEND SUCCESSFULLY------------")
        }
        producer?.close();
    }