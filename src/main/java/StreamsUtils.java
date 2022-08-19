
import com.google.protobuf.Message;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_KEY_TYPE;
import static io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE;

public class StreamsUtils {

    public static final String PROPERTIES_FILE_PATH = "src/main/resources/streams.properties";
    public static final short REPLICATION_FACTOR = 1;
    public static final int PARTITIONS = 1;

    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            properties.load(fis);
            return properties;
        }
    }
    public static Map<String,Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String)key, (String)value));
        return configs;
    }

    public static KafkaProtobufSerde<HypertheoryDocumentsEnrollment.Enrollment> getEnrollmentSerde(final Map<String, Object> serdeConfig) {
        final KafkaProtobufSerde<HypertheoryDocumentsEnrollment.Enrollment> protobufSerde = new KafkaProtobufSerde<>();
        serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE, HypertheoryDocumentsEnrollment.Enrollment.class.getName());
        protobufSerde.configure(serdeConfig,false);
        return protobufSerde;
    }

    public static KafkaProtobufSerde<HypertheoryDocumentsEnrollmentKey.EnrollmentKey> getEnrollmentKeySerde(final Map<String, Object> serdeConfig) {
        final KafkaProtobufSerde<HypertheoryDocumentsEnrollmentKey.EnrollmentKey> protobufSerde = new KafkaProtobufSerde<>();
        serdeConfig.put(SPECIFIC_PROTOBUF_KEY_TYPE, HypertheoryDocumentsEnrollmentKey.EnrollmentKey.class.getName());
        protobufSerde.configure(serdeConfig,false);
        return protobufSerde;
    }
    public static <T extends Message> KafkaProtobufSerde<T> getSpecificProtobufSerde(final Map<String, Object> serdeConfig) {
        final KafkaProtobufSerde<T> protobufSerde = new KafkaProtobufSerde<T>();
        protobufSerde.configure(serdeConfig, false);
        serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE, protobufSerde.getClass().getName());
        return protobufSerde;
    }

    public static Callback callback() {
        return (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }

        };
    }

    public static NewTopic createTopic(final String topicName){
        return new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
    }
}