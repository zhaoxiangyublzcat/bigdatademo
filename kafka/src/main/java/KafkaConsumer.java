import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者
 */
public class KafkaConsumer extends Thread {
    private String topic;


    public KafkaConsumer(String topic) {
        this.topic = topic;
    }

    public ConsumerConnector createCon() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaProperties.ZK);
        properties.put("group.id", KafkaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        super.run();
        ConsumerConnector connector = createCon();

        Map<String, Integer> map = new HashMap();
        map.put(topic, 1);
        // String：topic
        // List<KafkaStream<byte[], byte[]>>：数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = connector.createMessageStreams(map);
        KafkaStream<byte[], byte[]> messageAndMetadata = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = messageAndMetadata.iterator();
        while (iterator.hasNext()) {
            String result = new String(iterator.next().message());
            System.out.println(result);
        }

    }
}
