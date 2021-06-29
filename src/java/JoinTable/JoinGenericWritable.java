package JoinTable;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class JoinGenericWritable extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES =  (Class<? extends Writable>[]) new Class[] {
                PeopleFromDataRecord.class,
                SalaryFromDataRecord.class
        };
    }
    public JoinGenericWritable(Writable instance){
        set(instance);
    }
    public JoinGenericWritable() {}
    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
}
