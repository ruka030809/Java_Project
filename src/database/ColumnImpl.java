package database;

import java.util.ArrayList;
 import java.util.Iterator;
import java.util.List;

class ColumnImpl implements Column {
    String header;
    ArrayList<String> cell;

    int longgest = 0;

    ColumnImpl getColumnPartition(int startIndex, int endIndex){
        ColumnImpl column = new ColumnImpl(header);
        for(int i = startIndex; i<=endIndex; i++){
            column.cell.add(this.cell.get(i));
        }
        return column;
    }

    ColumnImpl selectRow(int... indices){
        ColumnImpl column = new ColumnImpl(header);
        for(int tmp : indices){
            column.cell.add(this.cell.get(tmp));
        }
        return column;
    }
    ColumnImpl selectRow(boolean[] indices){
        ColumnImpl column = new ColumnImpl(header);
        for(int i = 0; i<indices.length; i++){
            if(indices[i])
                column.cell.add(this.cell.get(i));
        }
        return column;
    }
    int nonNull(){
        int cnt = cell.size();
        for (String s : cell) if (s == null) cnt--;
        return cnt;
    }
    ColumnImpl(String header){
        this.header = header;
        cell = new ArrayList<>();
    }
    @Override
    public String getHeader() {
        return header;
    }

    @Override
    public String getValue(int index) {
        return cell.get(index);
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        String cellValue = cell.get(index);

        try {
            if (t == Double.class) {
                return t.cast(Double.parseDouble(cellValue));  // Double로 캐스팅하여 반환
            } else if (t == Integer.class) {
                return t.cast(Integer.parseInt(cellValue));  // Integer로 캐스팅하여 반환
            } else if (t == Long.class) {
                return t.cast(Long.parseLong(cellValue));  // Long으로 캐스팅하여 반환
            }
        }
        catch (Exception e){ return null; }
        return null;
    } // 얘는 아직 구현x

    @Override
    public void setValue(int index, String value) {
        this.cell.set(index,value);
    }

    @Override
    public void setValue(int index, int value) {
        this.setValue(index,String.valueOf(value));
    } // 모든 cell을 String으로 다루기 위해서 int값이 들어와도 String으로 변환해서 넣어줌

    @Override
    public int count() {
        return cell.size();
    }

    @Override
    public void show() {
        System.out.println(this.header);
        for(String tmp : cell){
            System.out.println(tmp);
        }
    }

    @Override
    public boolean isNumericColumn() {
        for (String s : cell) {
            try {
                if (s == null) continue;
                else Integer.parseInt(s);
            } catch (Exception e) {
                try {
                    Double.parseDouble(s);
                } catch (Exception f) {
                    return false;
                }
            }
        }
        return true;
//        int returnInt;
//        double returnDouble;
//        try {
//                returnInt = Integer.parseInt(cell.get(0));
//        }
//        catch(Exception e){
//            try{
//                returnDouble = Double.parseDouble(cell.get(0));
//            }
//            catch (Exception f){
//                return false;
//            }
//            return true;
//        }
//
//        return true;
    } // 다시 구현

    @Override
    public long getNullCount() {
        int cnt = 0;
        for(String tmp : cell){
            if(tmp == null) cnt++;
        }
        return cnt;
    }//대충 맞음 확인하기
}
