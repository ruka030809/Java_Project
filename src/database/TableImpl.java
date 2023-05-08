package database;

import database.Column;
import database.JoinColumn;
import database.Table;

import java.io.File;
import java.util.*;
import java.util.function.Predicate;

class TableImpl implements Table {

    String name;
    Set<ColumnImpl> columns = new HashSet<>();

    TableImpl(File f){
        String[] str = f.getName().split(".csv");
        name = str[0];
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
