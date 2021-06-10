package week2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

class ConsumerAvroWithKafka {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.140.0.13:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group2");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/employee.avro"));

        KafkaConsumer<String,byte[]>consumer = new KafkaConsumer<String, byte[]>(properties);

        consumer.subscribe(Arrays.asList("Avro-topic1"));
        while (true){
            ConsumerRecords<String,byte[]> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,byte[]> record : records){
                byte[] data = record.value();
                System.out.println(record.value());
                SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                GenericRecord user = reader.read(null,decoder);
                System.out.println(user);
            }
        }


    }
}