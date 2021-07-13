import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Configurator {
    public static final Dotenv dotenv = Dotenv.configure()
            .directory("./.env")
            .ignoreIfMalformed()
            .ignoreIfMissing()
            .load();
    public static final String brokers = dotenv.get("CLOUDKARAFKA_BROKERS");
    public static final String username = dotenv.get("CLOUDKARAFKA_USERNAME");
    public static final String password = dotenv.get("CLOUDKARAFKA_PASSWORD");
    public Properties props;
    public ArrayList<String> subscribedTopics;

    public Configurator() {
        this.subscribedTopics = new ArrayList<>(
                Arrays.asList(username + "-default",
                        username + "-picture-grayscale"
                ));
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);

        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", username + "-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", jaasCfg);
    }

}
