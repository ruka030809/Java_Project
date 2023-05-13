package database;

import database.Column;
import database.JoinColumn;
import database.Table;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.function.Predicate;

class TableImpl implements Table {

    String name;
    List<ColumnImpl> columns = new ArrayList<>();
    TableImpl(){ }
    TableImpl(File f) throws FileNotFoundException {
        String[] str = f.getName().split(".csv");
        name = str[0];
        Scanner sc = new Scanner(f);
        String line = sc.nextLine();
        String[] headers = line.split(",");
        for(int i = 0; i<headers.length; i++){
            ColumnImpl column = new ColumnImpl(headers[i]);
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
        table.name = this.getName();
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
        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            ColumnImpl column = new ColumnImpl(getName()+"."+getColumn(i).getHeader());
            for(int j = 0; j<getColumn(i).count(); j++){
                for(int k = 0; k<rightTable.getColumn(0).count(); k++){
                    if(getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                        column.cell.add(getColumn(i).getValue(j));
                }
            }
            table.columns.add(column);
        }
        for(int i = 0; i<rightTable.getColumnCount(); i++){
            ColumnImpl column = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader());
            for(int j = 0; j<getColumn(0).count(); j++){
                for(int k = 0; k<rightTable.getColumn(i).count(); k++){
                    if(getColumn(leftHeader).getValue(j).equals(rightTable.getColumn(rightHeader).getValue(k)))
                        column.cell.add(rightTable.getColumn(i).getValue(k));
                }
            }
            table.columns.add(column);
        }

        return table;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void show() {
        for(int i = 0; i<columns.size(); i++){
            columns.get(i).longgest = columns.get(i).header.length();
            for (int j = 0; j<columns.get(0).cell.size(); j++){
                if(columns.get(i).getValue(j) == null) columns.get(i).longgest = Math.max(4,columns.get(i).longgest);
                else columns.get(i).longgest = Math.max(columns.get(i).getValue(j).length(),columns.get(i).longgest);
            }
        }
        for(int i = 0; i<columns.size(); i++){
            System.out.printf(" %"+columns.get(i).longgest+"s |",columns.get(i).header);
        }
        System.out.println();

        for(int j = 0; j<columns.get(0).cell.size(); j++){
            for(int i = 0; i<columns.size(); i++){
                System.out.printf(" %"+columns.get(i).longgest+"s |",columns.get(i).getValue(j));
            }
            System.out.println();
        }

    }

    @Override
    public void describe() {
        System.out.println("<database.Table@"+Integer.toHexString(hashCode())+">");
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
        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(0,lineCount-1));
        }
        return table;
    }

    @Override
    public Table head(int lineCount) {
        lineCount = Math.min(lineCount,columns.get(0).cell.size());
        TableImpl table = new TableImpl();
        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(0,lineCount-1));
        }
        return table;
    }

    @Override
    public Table tail() {
        int lineCount = Math.min(5,columns.get(0).cell.size()); //4
        int startIndex = columns.get(0).cell.size() - lineCount;
        int endIndex = columns.get(0).cell.size() - 1;
        TableImpl table = new TableImpl();
        table.name = this.getName();
        for (int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(startIndex,endIndex));
        }
        return table;
    }

    @Override
    public Table tail(int lineCount) {
        lineCount = Math.min(lineCount,columns.get(0).cell.size()); //4
        int startIndex = columns.get(0).cell.size() - lineCount;
        int endIndex = columns.get(0).cell.size() - 1;
        TableImpl table = new TableImpl();
        table.name = this.getName();
        for (int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(startIndex,endIndex));
        }
        return table;

    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        TableImpl table = new TableImpl();
        table.name = this.getName();
        for (int i = 0; i<columns.size(); i++){
            table.columns.add(columns.get(i).getColumnPartition(beginIndex,endIndex-1));
        }
        return table;
    } // 예외 발생 가능

    @Override
    public Table selectRowsAt(int... indices) {
        TableImpl table = new TableImpl();
        table.name = this.getName();
        for(int i = 0; i<columns.size(); i++){

            table.columns.add(columns.get(i).selectRow(indices));

        }
        return table;
    }//다시

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        TableImpl table = new TableImpl();
        table.name = this.getName();
        for(int i = beginIndex; i<endIndex; i++){
            table.columns.add(this.columns.get(i));//
        }
        return table;
    } // 이거 칼럼도 새로 생생해서 add해주는 게 더 나을듯 아래 selectColumnsAt 도 똑같이 수정하기.

    @Override
    public Table selectColumnsAt(int... indices) {
        TableImpl table = new TableImpl();
        table.name = this.getName();
        for(int i : indices){
            table.columns.add(this.columns.get(i));//
        }
        return table;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst){
        final int columnIndex = byIndexOfColumn;

        final Comparator<String> comparator;
        if (isAscending) {
            comparator = Comparator.nullsLast(Comparator.naturalOrder());
        } else {
            comparator = Comparator.nullsFirst(Comparator.reverseOrder());
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
