package database;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.function.Predicate;

class TableImpl implements Table {

    String name;
    List<ColumnImpl> columns = new ArrayList<>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableImpl table = (TableImpl) o;
        return Objects.equals(name, table.name);

    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "database.Table@" + Integer.toHexString(hashCode());
    }

    TableImpl(){ }
    TableImpl(File f) throws FileNotFoundException {
        String[] str = f.getName().split(".csv");
        name = str[0];
        Scanner sc = new Scanner(f);
        String line = sc.nextLine();
        String[] headers = line.split(",");
        for (String header : headers) {
            ColumnImpl column = new ColumnImpl(header);
            columns.add(column);
        }
        while(sc.hasNext()){
            line = sc.nextLine();
            str = line.split(",");

            for(int i = 0; i<columns.size(); i++){
                try{
                    if(str[i].equals("")) columns.get(i).cell.add(null);

                    else columns.get(i).cell.add(str[i]);

                }
                catch (Exception e){
                    columns.get(i).cell.add(null);
                }
            }
        }
    }

    @Override
    public Table crossJoin(Table rightTable) {
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            ColumnImpl column = new ColumnImpl(getName()+"."+getColumn(i).getHeader());
            for(int j = 0; j<getColumn(i).count(); j++){
                for(int k = 0; k<rightTable.getColumn(0).count(); k++){
                    column.cell.add(getColumn(i).getValue(j));
                    //table.columns.get(i).cell.add(this.columns.get(i).cell.get(j));
                }
            }
            table.columns.add(column);
        }
        for(int i = 0; i<rightTable.getColumnCount(); i++){
            ColumnImpl column = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader());
            for(int j = 0; j<getColumn(0).count(); j++){
                for(int k = 0; k<rightTable.getColumn(i).count(); k++){
                    column.cell.add(rightTable.getColumn(i).getValue(k));
                }
            }
            table.columns.add(column);
        }

         return table;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        String leftHeader = joinColumns.get(0).getColumnOfThisTable();
        String rightHeader = joinColumns.get(0).getColumnOfAnotherTable();
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            ColumnImpl column = new ColumnImpl(getName()+"."+getColumn(i).getHeader());
            for(int j = 0; j<getColumn(i).count(); j++){
                for(int k = 0; k<rightTable.getColumn(0).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                            column.cell.add(getColumn(i).getValue(j));
                    }
                    catch (Exception e){}
                }
            }
            table.columns.add(column);
        }
        for(int i = 0; i<rightTable.getColumnCount(); i++){
            ColumnImpl column = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader());
            for(int j = 0; j<getColumn(0).count(); j++){
                for(int k = 0; k<rightTable.getColumn(i).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                            column.cell.add(rightTable.getColumn(i).getValue(k));
                    }
                    catch (Exception e){}
                }
            }
            table.columns.add(column);
        }

        return table;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        String leftHeader = joinColumns.get(0).getColumnOfThisTable();
        String rightHeader = joinColumns.get(0).getColumnOfAnotherTable();
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            ColumnImpl column = new ColumnImpl(getName()+"."+getColumn(i).getHeader());
            for(int j = 0; j<getColumn(i).count(); j++){
                for(int k = 0; k<rightTable.getColumn(0).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                            column.cell.add(getColumn(i).getValue(j));
                    }
                    catch (Exception e){}
                }
            }
            table.columns.add(column);
        }
        for(int i = 0; i<rightTable.getColumnCount(); i++){
            ColumnImpl column = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader());
            for(int j = 0; j<getColumn(0).count(); j++){
                for(int k = 0; k<rightTable.getColumn(i).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                            column.cell.add(rightTable.getColumn(i).getValue(k));
                    }
                    catch (Exception e){}
                }
            }
            table.columns.add(column);
        }
        boolean[] isAdded = new boolean[getRowCount()];
        for (int i = 0; i<getColumnCount(); i++){
            for(int j = 0; j<getColumn(0).count(); j++){
                for(int k = 0; k<rightTable.getColumn(0).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                            isAdded[j] = true;
                    }
                    catch (Exception e){}
                }
                if(!isAdded[j]) {
                    for (int k = 0; k < this.getColumnCount(); k++) {
                        table.columns.get(k).cell.add(this.columns.get(k).cell.get(j));
                    }
                    for (int k = 0; k < rightTable.getColumnCount(); k++) {
                        table.columns.get(k + this.getColumnCount()).cell.add(null);
                    }
                    isAdded[j] = true;
                }
            }
        }

        return table;
    } // 코드가 넘 비효율적임 수정하기

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        String leftHeader = joinColumns.get(0).getColumnOfThisTable();
        String rightHeader = joinColumns.get(0).getColumnOfAnotherTable();
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            ColumnImpl column = new ColumnImpl(getName()+"."+getColumn(i).getHeader());
            for(int j = 0; j<getColumn(i).count(); j++){
                for(int k = 0; k<rightTable.getColumn(0).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                            column.cell.add(getColumn(i).getValue(j));
                    }
                    catch (Exception e){}
                }
            }
            table.columns.add(column);
        }
        for(int i = 0; i<rightTable.getColumnCount(); i++){
            ColumnImpl column = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader());
            for(int j = 0; j<getColumn(0).count(); j++){
                for(int k = 0; k<rightTable.getColumn(i).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                            column.cell.add(rightTable.getColumn(i).getValue(k));
                    }
                    catch (Exception e){}
                }
            }
            table.columns.add(column);
        }
        boolean[] isAddedLeft = new boolean[getRowCount()];
        boolean[] isAddedRight = new boolean[rightTable.getRowCount()];
        for (int i = 0; i<getColumnCount(); i++){
            for(int j = 0; j<getColumn(0).count(); j++){
                for(int k = 0; k<rightTable.getColumn(0).count(); k++){
                    try {
                        if (getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k))) {
                            isAddedLeft[j] = true;
                            isAddedRight[k] = true;
                        }
                    }
                    catch (Exception e){}
                }
                if(!isAddedLeft[j]) {
                    for (int k = 0; k < this.getColumnCount(); k++) {
                        table.columns.get(k).cell.add(this.columns.get(k).cell.get(j));
                    }
                    for (int k = 0; k < rightTable.getColumnCount(); k++) {
                        table.columns.get(k + this.getColumnCount()).cell.add(null);
                    }
                    isAddedLeft[j] = true;
                }
            }
        }

        for(int i = 0; i<isAddedRight.length; i++){
            if(!isAddedRight[i]){
                for(int j = 0; j<getColumnCount(); j++){
                    table.columns.get(j).cell.add(null);
                }
                for(int j = 0; j<rightTable.getColumnCount(); j++){
                    table.columns.get(j+this.getColumnCount()).cell.add(rightTable.getColumn(j).getValue(i));
                }
            }
        }

        return table;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void show() {
        for(int i = 0; i<columns.size(); i++){
            columns.get(i).longest = columns.get(i).header.length();
            for (int j = 0; j<columns.get(0).cell.size(); j++){
                if(columns.get(i).getValue(j) == null) columns.get(i).longest = Math.max(4,columns.get(i).longest);
                else columns.get(i).longest = Math.max(columns.get(i).getValue(j).length(),columns.get(i).longest);
            }
        }
        for(int i = 0; i<columns.size(); i++){
            System.out.printf(" %"+columns.get(i).longest +"s |",columns.get(i).header);
        }
        System.out.println();

        for(int j = 0; j<columns.get(0).cell.size(); j++){
            for(int i = 0; i<columns.size(); i++){
                System.out.printf(" %"+columns.get(i).longest +"s |",columns.get(i).getValue(j));
            }
            System.out.println();
        }

    }

    @Override
    public void describe() {
        System.out.println("<"+this+">");
        System.out.println("RangeIndex: " + columns.get(0).cell.size() +" entries, 0 to " + (columns.get(0).cell.size()-1));
        System.out.println("Data columns (total " + columns.size() + " columns):");
        int maxNum = (""+columns.size()).length();
        int maxCol = 6;
        int maxNon = 14;
        for(int i = 0; i<columns.size(); i++){
            maxCol = Math.max(maxCol,columns.get(i).getHeader().length());
            maxNon = Math.max(maxNon, (columns.get(i).nonNull()+" non-null" ).length());
        }
        System.out.printf(" %"+maxNum+"s |" + " %" +maxCol+"s |" + " %" +maxNon+"s |" +" Dtype","#","Column","Non-Null Count");
        System.out.println();
        for(int i = 0; i<columns.size(); i++){
            System.out.printf(" %"+maxNum+"s |" + " %" +maxCol+"s |" + " %" +maxNon+"s |" +" %s",""+i,columns.get(i).header,columns.get(i).nonNull()+" non-null", columns.get(i).isNumericColumn()?"int":"String");
            System.out.println();
        }
        int intCnt = 0, StringCnt = 0;
        for (int i = 0; i<columns.size(); i++){
            if(columns.get(i).isNumericColumn()) intCnt++;
            else StringCnt++;
        }

        System.out.printf("dtypes: int(%d), String(%d)", intCnt, StringCnt);
        System.out.println();
    }

    @Override
    public Table head() {
        int lineCount = Math.min(5,columns.get(0).cell.size());
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(0,lineCount));
        }
        return table;
    }

    @Override
    public Table head(int lineCount) {
        lineCount = Math.min(lineCount,columns.get(0).cell.size());
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(0,lineCount));
        }
        return table;
    }

    @Override
    public Table tail() {
        int lineCount = Math.min(5,columns.get(0).cell.size()); //4
        int startIndex = columns.get(0).cell.size() - lineCount;
        int endIndex = columns.get(0).cell.size();
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for (int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(startIndex,endIndex));
        }
        return table;
    }

    @Override
    public Table tail(int lineCount) {
        lineCount = Math.min(lineCount,columns.get(0).cell.size()); //4
        int startIndex = columns.get(0).cell.size() - lineCount;
        int endIndex = columns.get(0).cell.size();
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for (int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(startIndex,endIndex));
        }
        return table;

    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for (int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(beginIndex,endIndex));
        }
        return table;
    } // 예외 발생 가능

    @Override
    public Table selectRowsAt(int... indices) {
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){

            table.columns.add(columns.get(i).selectRow(indices));

        }
        return table;
    }//다시

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i = beginIndex; i<endIndex; i++){
//            ColumnImpl column = new ColumnImpl(columns.get(i).getHeader());
//            for(int j = 0; j<columns.get(i).count(); j++){
//                column.cell.add(columns.get(i).getValue(j));
//            }
            ColumnImpl column = columns.get(i).getColumnPartition(0,columns.get(i).count());
            table.columns.add(column);//
        }
        return table;
    } // 이거 칼럼도 새로 생생해서 add해주는 게 더 나을듯 아래 selectColumnsAt 도 똑같이 수정하기.

    @Override
    public Table selectColumnsAt(int... indices) {
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        for(int i : indices){
            ColumnImpl column = columns.get(i).getColumnPartition(0,columns.get(i).count());
            table.columns.add(column);//
        }
        return table;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        TableImpl table = new TableImpl();
//        table.name = this.getName();
        Column column = getColumn(columnName);
        boolean[] a = new boolean[getRowCount()];
        if(!column.isNumericColumn()) {
            for (int i = 0; i < a.length; i++) {

                try {
                    if (predicate.test((T) column.getValue(i))) a[i] = true;
                }
                catch (Exception e) { }
            }
        }
        else{
            for (int i = 0; i < a.length; i++) {
                try {
                    if (predicate.test((T) column.getValue(i,Integer.class))) a[i] = true;
                }
                catch (Exception e){}
            }
        }
        for(int i = 0; i<getColumnCount(); i++){
            table.columns.add(columns.get(i).selectRow(a));
        }
        return table;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst){
        final int columnIndex = byIndexOfColumn;

        final Comparator<String> comparator;
        if (isAscending) {
            if(isNullFirst) comparator = Comparator.nullsFirst(Comparator.naturalOrder());
            else comparator = Comparator.nullsLast(Comparator.naturalOrder());
        } else {
            if(isNullFirst) comparator = Comparator.nullsFirst(Comparator.reverseOrder());
            else comparator = Comparator.nullsLast(Comparator.reverseOrder());
        }


        List<String[]> rows = new ArrayList<>();
        int numRows = columns.get(0).cell.size();
        for (int i = 0; i < numRows; i++) {
            String[] row = new String[columns.size()];
            for (int j = 0; j < columns.size(); j++) {
                row[j] = columns.get(j).cell.get(i);
            }
            rows.add(row);
        }

        rows.sort((row1, row2) -> comparator.compare(row1[columnIndex], row2[columnIndex]));

        for (int i = 0; i < columns.size(); i++) {
            List<String> cell = columns.get(i).cell;
            for (int j = 0; j < numRows; j++) {
                cell.set(j, rows.get(j)[i]);
            }
        }

        return this;
    }

    @Override
    public int getRowCount() {
        return columns.get(0).cell.size();
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public Column getColumn(int index) {
        return columns.get(index);
    }

    @Override
    public Column getColumn(String name) {
        for (Column tmp: columns){
            if(tmp.getHeader().equals(name)) return tmp;
        }
        return null;
    }
}
