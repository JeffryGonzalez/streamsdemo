import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;


import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import java.util.Properties;
import static org.apache.kafka.common.serialization.Serdes.Double;
import static org.apache.kafka.common.serialization.Serdes.Long;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class DemoOne {



    public static void main(String[] args)  {


    }
}
