package week4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;


public class WordNumber {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text,Text,IntWritable>{
        private static  Text word = new Text();
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector< Text,IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String []data = line.split("\\s+");
            for (String number : data){
                outputCollector.collect(new Text("0"),new IntWritable(Integer.valueOf(number)));
            }

        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
        private static Set<String>listnumber = new LinkedHashSet<>();

        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            while (iterator.hasNext()){
                listnumber.add(iterator.next().toString());
            }
            outputCollector.collect(new Text("0"),new IntWritable(listnumber.size()));
        }
    }
    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(WordNumber.class);
        job.setJobName("WordNumber");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        JobClient.runJob(job);
    }
}
