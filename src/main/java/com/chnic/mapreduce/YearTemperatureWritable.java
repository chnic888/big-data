package com.chnic.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearTemperatureWritable implements WritableComparable<YearTemperatureWritable> {

    private int year;

    private float temperature;

    public YearTemperatureWritable() {

    }

    public YearTemperatureWritable(int year, float temperature) {
        this.year = year;
        this.temperature = temperature;
    }

    public int getYear() {
        return year;
    }

    public float getTemperature() {
        return temperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearTemperatureWritable that = (YearTemperatureWritable) o;
        return year == that.year &&
                Float.compare(that.temperature, temperature) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, temperature);
    }

    @Override
    public String toString() {
        return "YearTemperatureWritable{" +
                "year=" + year +
                ", temperature=" + temperature +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeFloat(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        temperature = dataInput.readFloat();
    }

    @Override
    public int compareTo(YearTemperatureWritable yearTemperatureWritable) {
        int result = Integer.compare(year, yearTemperatureWritable.year);
        if (result != 0) {
            return result;
        }

        return Float.compare(temperature, yearTemperatureWritable.temperature);
    }
}
