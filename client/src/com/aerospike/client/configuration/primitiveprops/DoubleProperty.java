package com.aerospike.client.configuration.primitiveprops;

public class DoubleProperty {
    public int value;

    public DoubleProperty() {}

    public DoubleProperty(int value) {
        this.value = value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
