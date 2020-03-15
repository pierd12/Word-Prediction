package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ValuePair implements WritableComparable<ValuePair> {
       
	Text key;
	DoubleWritable value;
	public ValuePair() {
		key= new Text("");
		value = new DoubleWritable(0);
		// TODO Auto-generated constructor stub
	}
	public ValuePair(String st , double val) {
		key= new Text(st);
		value = new DoubleWritable(val);
		// TODO Auto-generated constructor stub
	}
	public String getKey()
	{
		return key.toString();
	}
	public double getValue()
	{
		return value.get();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
		
	}
	

	@Override
	public int compareTo(ValuePair other) {
		int firstCompare = this.getKey().compareTo(other.getKey());
        if(firstCompare != 0) {
            return firstCompare;
        }
        if(this.getValue() > other.getValue())
        	return -1;
        if(this.getValue()==other.getValue())
        	return 0;
        return 1;
	     
		
	}
	@Override
	public String toString()
	{
		return key.toString()+'\t'+Double.toString(value.get());
		
	}

}
