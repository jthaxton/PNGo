import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import photoProcessor.PhotoProcessor;
import org.json.*;

public class PNGo {
    public static final String GRAYSCALE = "GRAYSCALE";

    private Configurator config = new Configurator();

    public PNGo() {
    }

    public void consume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.props);
        consumer.subscribe(config.subscribedTopics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                String recordTopic = record.topic();
                String message = record.value();
                JSONObject messageObject = new JSONObject(message);
                //        {
                //            "url": String,
                //            "action": String
                //        }
                switch (messageObject.get("action").toString()) {
                    case GRAYSCALE:
                        new PhotoProcessor(messageObject);
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
//        Thread one = new Thread() {
//            public void run() {
//                try {
//                    Producer<String, String> producer = new KafkaProducer<>(config.props);
//                    int i = 0;
//                    while(true) {
//                        Date d = new Date();
//                        producer.send(new ProducerRecord<>("topic", Integer.toString(i), d.toString()));
//                        Thread.sleep(1000);
//                        i++;
//                    }
//                } catch (InterruptedException v) {
//                    System.out.println(v);
//                }
//            }
//        };
//        one.start();
    }

    public static void main(String[] args) {
        PNGo c = new PNGo();
        c.consume();
    }
}
