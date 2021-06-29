package JoinTable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProfessionID implements WritableComparable<ProfessionID> {
    public Text profession = new Text();
    public IntWritable id = new IntWritable();

    public ProfessionID(String professor, IntWritable id) {
        this.profession.set(professor);
        this.id.set(id.get());
    }
    public ProfessionID(){}

    @Override
    public int compareTo(ProfessionID o) {
        if(this.profession.equals(o.profession)){
            return this.id.compareTo(o.id);
        }
        else {
            return this.profession.compareTo(o.profession);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.profession.write(dataOutput);
        this.id.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.profession.readFields(dataInput);
        this.id.readFields(dataInput);
    }

    public boolean equals(ProfessionID pro) {

        return this.profession.equals(pro.profession) && this.id.equals(pro.id);
    }

    @Override
    public int hashCode() {
        return this.profession.hashCode();
    }
}
