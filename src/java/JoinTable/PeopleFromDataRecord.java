package JoinTable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PeopleFromDataRecord implements Writable {
    public IntWritable id = new IntWritable();
    public Text firstName = new Text();
    public Text lastName = new Text();
    public Text email = new Text();
    public Text city = new Text();
    public IntWritable fieldName = new IntWritable();

    public PeopleFromDataRecord(int id, String firstName, String lastName, String email, String city, int fieldName) {
        this.id.set(id);
        this.firstName.set(firstName);
        this.lastName.set(lastName);
        this.email.set(email);
        this.city.set(city);
        this.fieldName.set(fieldName);
    }
    public PeopleFromDataRecord(){

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.id.write(dataOutput);
        this.firstName.write(dataOutput);
        this.lastName.write(dataOutput);
        this.city.write(dataOutput);
        this.email.write(dataOutput);
        this.fieldName.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id.readFields(dataInput);
        this.firstName.readFields(dataInput);
        this.lastName.readFields(dataInput);
        this.city.readFields(dataInput);
        this.email.readFields(dataInput);
        this.fieldName.readFields(dataInput);
    }
}
