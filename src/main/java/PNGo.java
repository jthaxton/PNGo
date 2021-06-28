import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import photoProcessor.PhotoProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class PNGo {
    private final String topic;
    private final Properties props;
    private final ArrayList<String> topics;

    public PNGo(String brokers, String username, String password) {
        this.topic = username + "-default";
        this.topics = new ArrayList<String>(
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

    public void consume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                String recordTopic = record.topic();
                switch (recordTopic) {
                    case "1ii7hnjw-default":
                        new PhotoProcessor(record);
                        break;
                    case "1ii7hnjw-picture-grayscale":
                        new PhotoProcessor(record);
                        break;
                    default:
                        System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n",
                                record.topic(), record.partition(),
                                record.offset(), record.key(), record.value());
                }

			}
        }
    }

    public void produce() {
        Thread one = new Thread() {
            public void run() {
                try {
                    Producer<String, String> producer = new KafkaProducer<>(props);
                    int i = 0;
                    while(true) {
                        Date d = new Date();
                        producer.send(new ProducerRecord<>(topic, Integer.toString(i), d.toString()));
                        Thread.sleep(1000);
                        i++;
                    }
                } catch (InterruptedException v) {
                    System.out.println(v);
                }
            }
        };
        one.start();
    }

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure()
                .directory("./.env")
                .ignoreIfMalformed()
                .ignoreIfMissing()
                .load();
		String brokers = dotenv.get("CLOUDKARAFKA_BROKERS");
		String username = dotenv.get("CLOUDKARAFKA_USERNAME");
		String password = dotenv.get("CLOUDKARAFKA_PASSWORD");
        PNGo c = new PNGo(brokers, username, password);
//        c.produce();
        c.consume();
    }
}
