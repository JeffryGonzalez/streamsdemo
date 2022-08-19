import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class DemoOne {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, String> stream = builder.stream("demo1");

        stream.foreach((key, value) -> System.out.println("Hello, " + value));


       HypertheoryEvents.EnrollmentOrBuilder b = HypertheoryEvents.Enrollment.newBuilder()
               .setAccountRepId("Jeff")
               .setOfferingId("12");



    }
}
