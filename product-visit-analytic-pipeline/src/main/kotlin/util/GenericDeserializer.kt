import com.google.gson.FieldNamingPolicy
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer
import java.lang.reflect.Type


class GenericDeserializer<T> : Deserializer<T?> {
    private val gson: Gson = GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create()
    private var destinationClass: Class<T>? = null
    private var reflectionTypeToken: Type? = null

    /** Default constructor needed by Kafka  */
    constructor(destinationClass: Class<T>?) {
        this.destinationClass = destinationClass
    }

    constructor(reflectionTypeToken: Type?) {
        this.reflectionTypeToken = reflectionTypeToken
    }

    override fun configure(props: Map<String?, *>?, isKey: Boolean) {}
    override fun deserialize(topic: String, bytes: ByteArray): T? {
        if (bytes == null) {
            return null
        }
        val type = if (destinationClass != null) destinationClass!! else reflectionTypeToken!!
        return gson.fromJson(String(bytes), type)
    }

    override fun close() {}
}
