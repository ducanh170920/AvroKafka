package JoinTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Driver {
    public static final IntWritable SALARY_RECORD = new IntWritable(0);
    public static final IntWritable DATA_RECORD = new IntWritable(1);
    public static class JoinGroupingComparator extends WritableComparator {
        public JoinGroupingComparator() {
            super(ProfessionID.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ProfessionID first = (ProfessionID) a;
            ProfessionID second = (ProfessionID) b;
            return first.profession.compareTo(second.profession);
        }

    }
    public static class JoinSortingComparator extends WritableComparator {
        public JoinSortingComparator()
        {
            super (ProfessionID.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            ProfessionID first = (ProfessionID) a;
            ProfessionID second = (ProfessionID) b;

            return first.compareTo(second);
        }
    }
    public static class PeopleDataMapper extends  Mapper<LongWritable, Text, ProfessionID, JoinGenericWritable> {


        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            String[] recordFile = text.toString().split(",");
            int id = Integer.parseInt(recordFile[0]);
            String firstName = recordFile[1];
            String lastName = recordFile[2];
            String email = recordFile[3];
            String city = recordFile[4];
            String professor = recordFile[5];
            int fieldName = Integer.parseInt(recordFile[6]);

            ProfessionID recordKey = new ProfessionID(professor,DATA_RECORD);
            PeopleFromDataRecord record = new PeopleFromDataRecord(id,firstName,lastName,email,city,fieldName);

            JoinGenericWritable genericWritable = new JoinGenericWritable(record);
            context.write(recordKey,genericWritable);

        }

    }
    public static class SalaryMapper extends Mapper<LongWritable,Text,ProfessionID,JoinGenericWritable> {

        public void map(LongWritable longWritable, Text text,  Context context) throws IOException, InterruptedException {
            String[] recordFields = text.toString().split(",");
            String professor = recordFields[0];
            int salary = Integer.parseInt(recordFields[1]);
            ProfessionID professionID = new ProfessionID(professor,SALARY_RECORD);
            SalaryFromDataRecord salaryFromDataRecord = new SalaryFromDataRecord(salary);
            JoinGenericWritable genericWritable = new JoinGenericWritable(salaryFromDataRecord);
            context.write(professionID,genericWritable);
        }
    }
    public static class JoinReduce extends Reducer<ProfessionID,JoinGenericWritable,NullWritable,Text> {
        private String header = "id,city,email,firstName,lastName,fieldName,Salary";
        private Boolean is_header = true;
        public void reduce(ProfessionID professionID, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException {
                StringBuilder output = new StringBuilder();
                String salary = "";
                if(is_header){
                    context.write(NullWritable.get(),new Text(header));
                    is_header = false;
                }


                for (JoinGenericWritable v : values){
                    Writable record = v.get();
                    if(professionID.id.equals(SALARY_RECORD)){
                       SalaryFromDataRecord record1 = (SalaryFromDataRecord) record;
                       salary = Integer.toString(record1.salary.get());
                    }else

                    {
                        PeopleFromDataRecord record1 = (PeopleFromDataRecord)record;
                        output.append(record1.id).append(",");
                        output.append(record1.city).append(",");
                        output.append(record1.email).append(",");
                        output.append(record1.firstName).append(",");
                        output.append(record1.lastName).append(",");
                        output.append(record1.fieldName).append(",");
                        output.append(salary);
                        context.write(NullWritable.get(),new Text(output.toString()));
                        output.delete(0,output.length());
                    }
                }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf,"join");
        job.setJarByClass(Driver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(ProfessionID.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PeopleDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SalaryMapper.class);

        job.setReducerClass(JoinReduce.class);

        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}