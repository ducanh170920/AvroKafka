
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


public class Table1 {
    public static  class Map extends MapReduceBase implements Mapper<LongWritable, Text,Text, Text> {

        private Text word = new Text();

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String []stringArr = text.toString().split(",");
            String s = text.toString();
            if (stringArr[5].equals("doctor")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+10000000));
            }
            if (stringArr[5].equals("police officer")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+18000000));
            }
            if (stringArr[5].equals("worker")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+12000000));
            }
            if (stringArr[5].equals("developer")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+14000000));
            }
            if (stringArr[5].equals("teacher")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+15000000));
            }
            if (stringArr[5].equals("farmer")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+6000000));
            }
            if (stringArr[5].equals("baker")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+10000000));
            }
            if (stringArr[5].equals("firefighter")){
                outputCollector.collect(new Text(stringArr[5]),new Text(s+","+11000000));
            }
            if (stringArr[5].equals("driver")) {
                outputCollector.collect(new Text(stringArr[5]),new Text(s+4000000));
            }

        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text,Text, NullWritable,Text> {

        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<NullWritable, Text> outputCollector, Reporter reporter) throws IOException {

            while (iterator.hasNext()) {
                outputCollector.collect(NullWritable.get(), iterator.next());
            }

        }

    }

    public static void main(String[] args) throws IOException {
        JobConf jobConf = new JobConf(Table1.class);
        jobConf.setJobName("table");
        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setMapperClass(Table1.Map.class);
        jobConf.setReducerClass(Table1.Reduce.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf,new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf,new Path(args[1]));
        JobClient.runJob(jobConf);
    }
}
