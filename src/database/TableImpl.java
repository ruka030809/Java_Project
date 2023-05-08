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
         return null;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
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
            for (int j = 0; j<columns.size(); j++){
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

    }

    @Override
    public Table head() {
        return null;
    }

    @Override
    public Table head(int lineCount) {
        return null;
    }

    @Override
    public Table tail() {
        return null;
    }

    @Override
    public Table tail(int lineCount) {
        return null;
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectRowsAt(int... indices) {
        return null;
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        return null;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        return null;
    }

    @Override
    public int getRowCount() {
        return 0;
    }

    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public Column getColumn(int index) {
        return null;
    }

    @Override
    public Column getColumn(String name) {
        return null;
    }
}
