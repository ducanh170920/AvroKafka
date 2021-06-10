package week2;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.net.www.content.text.Generic;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class ProducerAvroWithKafkaRegistry {
    public static void main(String[] args) throws IOException {
        //config pro
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.13:8081");

        //create producer
        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("src/main/resources/employee.avro"));

        GenericRecord employee = new GenericData.Record(schema);
        GenericRecord address = new GenericData.Record(schema.getField("address").schema());
        address.put("pinCode",123);
        address.put("streetName","Hai Phong");

        employee.put("ID",1231231);
        employee.put("email","ducanhchelseafc@gmail.com");
        employee.put("name","anhhd25");
        employee.put("address",address);


        ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>("anhhd25_serilizer3", null, employee);
        System.out.println(employee);

        producer.send(producerRecord);
        producer.flush();
        producer.close();
        System.out.println("send success");
    }
}
