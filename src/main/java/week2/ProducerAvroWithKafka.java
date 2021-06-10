package week2;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import java.io.*;
import java.util.Properties;
import java.util.Scanner;

public class ProducerAvroWithKafka {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());


        Schema.Parser parser = new Schema.Parser();
        Schema  schema = parser.parse(new File("src/main/resources/employee.avro"));

        GenericRecord employee = new GenericData.Record(schema);
        GenericRecord address = new GenericData.Record(schema.getField("address").schema());
        address.put("pinCode",123);
        address.put("streetName","Hai Phong");

        employee.put("ID",1231231);
        employee.put("email","ducanhchelseafc@gmail.com");
        employee.put("name","anhhd25");
        employee.put("address",address);

        System.out.println(employee);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream,null);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(employee,encoder);
        encoder.flush();
        outputStream.close();
        byte[] serializeByte = outputStream.toByteArray();
        System.out.println(serializeByte);
        System.out.println(serializeByte.toString());


        KafkaProducer<String,byte[]> producer = new KafkaProducer<String, byte[]>(properties);
        ProducerRecord<String,byte[]> producerRecord = new ProducerRecord<String,byte[]>("Avro-topic1",null,serializeByte);

        producer.send(producerRecord);
        producer.flush();
        producer.close();





    }
}
