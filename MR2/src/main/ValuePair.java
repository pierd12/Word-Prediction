package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ValuePair implements WritableComparable<ValuePair> {
       
	Text key;
	IntWritable value;
	public ValuePair() {
		key= new Text("");
		value = new IntWritable(0);
		// TODO Auto-generated constructor stub
	}
	public ValuePair(String st , int val) {
		key= new Text(st);
		value = new IntWritable(val);
		// TODO Auto-generated constructor stub
	}
	public String getKey()
	{
		return key.toString();
	}
	public int getValue()
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
        return this.getValue()-(other.getValue());
	     
		
	}
	@Override
	public String toString()
	{
		return key.toString()+'\t'+Integer.toString(value.get());
		
	}

}
