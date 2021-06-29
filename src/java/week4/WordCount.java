package week4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount {
    public static  class Map extends MapReduceBase implements Mapper<LongWritable, Text,Text, IntWritable>{
       private final  static IntWritable one = new IntWritable(1);
       private Text word = new Text();

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String format = "";
            String []data = line.split("\\.|\\s|\\'|\\,|\\?");
            for (String d : data){
                word.set(d+",");
                outputCollector.collect(word,one);
            }

        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()) {
                sum += iterator.next().get();
            }
            outputCollector.collect(text, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws IOException {
        JobConf jobConf = new JobConf(WordCount.class);
        jobConf.setJobName("countWord");
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(Map.class);
        jobConf.setCombinerClass(Reduce.class);
        jobConf.setReducerClass(Reduce.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf,new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf,new Path(args[1]));
        JobClient.runJob(jobConf);
    }
}
