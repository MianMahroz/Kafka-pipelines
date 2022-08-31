import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class DataProducer

    var products = listOf<String>("POSTPAID_PLAN_150","IPHONE_14","PREPAID_PLAN_350","IPAD")

    fun main() {
        run()
    }

fun setUpProducer(): KafkaProducer<String,String> {
    var props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"

    return KafkaProducer<String,String>(props);
}

fun run() {
    var producer = setUpProducer()
    println("------PRODUCER SETUP COMPLETED--------")



    for (i in 1..1000) {

        // CSV DATA i.e 144,IPHONE_14,1,3
        val productId = Random().nextInt(900)
        val productName = products[Random().nextInt(products.size)]
        val eventType = "CLICK"
//        val minutesUserStayed: Int = Random().nextInt(10)

        var value = "$productId,$productName,$eventType"

        var record = ProducerRecord("product.click.events",productName,value)

        producer.send(record)

        Thread.sleep(2000)
        println("-------SENDING RECORD--------")
        println("$record")
    }



}


