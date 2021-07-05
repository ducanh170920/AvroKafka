package week5;


import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.Sequence;
import org.apache.spark.sql.functions.*;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.collect_set;
public class Spark {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
//                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.read().parquet("hdfs:/user/anhhd25/input/Sample_data.parquet");
        df.createOrReplaceTempView("transaction");

        //Ex3a

        df = spark.sql("select * from transaction where device_model is not null");
        Dataset<Row> deviceModelNumUser = spark.sql("SELECT device_model, count(user_id) as count FROM transaction GROUP BY device_model");
        deviceModelNumUser.repartition(1).write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("hdfs://10.140.0.13:9000/user/anhhd25/device_model_num_user");

        //Ex3b

        Dataset<Row>deviceModelListUser = spark.sql("select device_model," +
                "concat_ws(',',collect_set(user_id)) as list_user_id " +
                " from transaction group by device_model");
        deviceModelListUser.repartition(1).write().mode(SaveMode.Overwrite)
                .option("compression", "snappy")
                .orc("hdfs://10.140.0.13:9000/user/anhhd25/device_model_list_user");

        // Ex4

        Dataset<Row> actionByButtonId = spark.sql("select concat(user_id,'_',device_model) as user_id_device_model,\n" +
                "\tbutton_id,count(*) as count \n" +
                "\tfrom transaction\n" +
                "\twhere button_id is not null\n" +
                "\tgroup by user_id_device_model,button_id");
        actionByButtonId.repartition(1).write().mode(SaveMode.Overwrite)
                .option("compression", "snappy")
                .parquet("hdfs://10.140.0.13:9000/user/anhhd25/action_by_button_id");
    }
}
