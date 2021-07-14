package week6;

import com.github.javafaker.Faker;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.spark.unsafe.types.ByteArray;

import java.io.BufferedInputStream;
import java.io.Console;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

public class Producer {

    public static void main(String[] args) throws IOException {

        Faker faker = new Faker(Locale.forLanguageTag("vi"));

        Random generator = new Random(5);
        LocalTime time = LocalTime.MIN.plusSeconds(generator.nextLong());

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);
        ProducerRecord<String, byte[]> producerRecord = null;

        long offset = Timestamp.valueOf("2022-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2025-01-01 00:00:00").getTime();
        long diff = end - offset + 1;

        for(int i = 0 ; i < 100 ; i++){
            Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));

            Message.DataTracking message = Message.DataTracking.newBuilder()
                    .setVersion(String.valueOf(faker.number().numberBetween(1,100)))
                    .setName(faker.name().fullName())
                    .setTimestamp(rand.getTime())
                    .setPhoneId(faker.phoneNumber().phoneNumber())
                    .setLon(faker.number().numberBetween(1,100))
                    .setLat(faker.number().numberBetween(1,100))
                    .build();
            producerRecord = new ProducerRecord<String, byte[]>("data_tracking_anhhd25", null, message.toByteArray());
            producer.send(producerRecord);
        }
//


        producer.flush();
        producer.close();


    }
}
