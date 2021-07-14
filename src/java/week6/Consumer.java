package week6;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.beans.Expression;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;


public class Consumer {
    public static Timestamp getTime(long time){
        Timestamp rand = new Timestamp(time);
        return rand;
    }
    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException, StreamingQueryException {


        SparkSession session = SparkSession.builder()
                .appName("SparkKafka")
                .getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        session.streams().awaitAnyTermination(1000);
        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","10.140.0.13:9092")
                .option("subscribe","data_tracking_anhhd25")
                .option("group.id","group1")
                .option("startingOffsets", "earliest")
                .option("auto.offset.reset","true")
                .option("value.serializer","org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .load();
        Dataset<byte[]>words = df.select("value").as(Encoders.BINARY());
        Dataset<String> object = words.map((MapFunction<byte[], String>)
                s-> (Message.DataTracking.parseFrom(s).getVersion()+
                        "#"+ Message.DataTracking.parseFrom(s).getName()+
                        "#"+ Message.DataTracking.parseFrom(s).getTimestamp()+
                        "#"+ Message.DataTracking.parseFrom(s).getPhoneId()+
                        "#"+ Message.DataTracking.parseFrom(s).getLon()+
                        "#"+ Message.DataTracking.parseFrom(s).getLat()+
                        "#"+ (getTime(Message.DataTracking.parseFrom(s).getTimestamp()).getYear()+1900)+
                        "#"+ (getTime(Message.DataTracking.parseFrom(s).getTimestamp()).getMonth()+1)+
                        "#"+ getTime(Message.DataTracking.parseFrom(s).getTimestamp()).getDate()+
                        "#"+ getTime(Message.DataTracking.parseFrom(s).getTimestamp()).getHours()
                ),Encoders.STRING());

        Dataset<Row> result = object.withColumn("version", functions.split(object.col("value"), "#").getItem(0))
                .withColumn("name", functions.split(object.col("value"), "#").getItem(1))
                .withColumn("timestamp", functions.split(object.col("value"), "#").getItem(2))
                .withColumn("phone_id", functions.split(object.col("value"), "#").getItem(3))
                .withColumn("lon", functions.split(object.col("value"), "#").getItem(4))
                .withColumn("lat", functions.split(object.col("value"), "#").getItem(5))
                .withColumn("year", functions.split(object.col("value"), "#").getItem(6))
                .withColumn("month", functions.split(object.col("value"), "#").getItem(7))
                .withColumn("date", functions.split(object.col("value"), "#").getItem(8))
                .withColumn("hour", functions.split(object.col("value"), "#").getItem(9))
                .drop("value");

//        result.join(tmp, expr)
        StreamingQuery query = result
                .selectExpr("CAST(name AS STRING)","CAST(version AS STRING)","CAST(timestamp AS STRING)","CAST(phone_id AS STRING)","CAST(lon AS STRING)","CAST(year AS STRING)","CAST(month AS STRING)"
                ,"CAST (date AS STRING)","CAST(hour AS STRING)").coalesce(1)
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("compression", "snappy")
                .option("path","hdfs://10.140.0.13:9000/user/anhhd25/data_tracking")
                .option("checkpointLocation","hdfs://10.140.0.13:9000/user/anhhd25/")
                .partitionBy("year","month","date","hour")
                .start();

        query.awaitTermination();

  }

}
