package JoinTable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalaryFromDataRecord implements Writable{
    public IntWritable salary = new IntWritable();
    public SalaryFromDataRecord(){

    }
    public SalaryFromDataRecord(int salary) {
        this.salary.set(salary);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        salary.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        salary.readFields(dataInput);
    }
}
