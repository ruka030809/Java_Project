package database;

import java.util.ArrayList;
import java.util.List;

class ColumnImpl implements Column {
    String header;
    ArrayList<String> value;

    public String getHeader() {
        return header;
    }

    @Override
    public String getValue(int index) {
        return value.get(index);
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        return null;
    }

    @Override
    public void setValue(int index, String value) {
        this.value.set(index,value);
    }

    @Override
    public void setValue(int index, int value) {
        this.setValue(index,String.valueOf(value));
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public void show() {

    }

    @Override
    public boolean isNumericColumn() {
        return false;
    }

    @Override
    public long getNullCount() {
        return 0;
    }
}
