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
    int nonNull(){
        int cnt = cell.size();
        for(int i = 0; i< cell.size(); i++)
            if(cell.get(i) == null) cnt--;
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
        for(int i = 0; i< cell.size(); i++){
            try {
                if(cell.get(i) == null) continue;
                else Integer.parseInt(cell.get(i));
            }
            catch (Exception e){
                try {
                    Double.parseDouble(cell.get(i));
                }
                catch (Exception f){
                    return false;
                }
            }
            return true;
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
