package datastructures.projects.database;


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SelectQuery;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArrayListTable implements Serializable {
    private String tableName;
    private ArrayList<ArrayList> table;
    private HashMap<String, Integer> colNameMap;
    private int columnCount;
    private ArrayList<HashSet> columnUnique;
    private HashMap<String, Boolean> primaryColumnName;
    private ArrayList<String> columnNames = new ArrayList<String>();


    public ArrayListTable(String tableName) {
        this.tableName = tableName;
        table = new ArrayList<>();
        colNameMap = new HashMap<>();
        columnCount = 0;
        columnUnique = new ArrayList<>();
        primaryColumnName = new HashMap<>();


    }

    public String getTableName() {
        return tableName;
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(ArrayList<String> columnNames) {
        this.columnNames = columnNames;
    }

    public int size() {return table.size(); }

    public ArrayList<ArrayList> getTable() {
        return table;
    }

    public void setTableName(String name) {
        tableName = name;
    }

    public void setPrimaryColumnName(String columnName){
        primaryColumnName.put(columnName, true);
    }

    public HashMap<String, Boolean> getPrimaryColumnName() {
        return primaryColumnName;
    }

    public HashMap<String, Integer> getColNameMap() {
        return colNameMap;
    }

    public boolean addRow(ArrayList rowOfValues, ArrayList<String> rowOfColumnNames) {

        Object[] tempArray = new Object[columnCount];
        for (int i = 0; i < rowOfColumnNames.size(); i++) {
            Object rowValue = rowOfValues.get(i);
            String columnName = rowOfColumnNames.get(i);
            if (colNameMap.containsKey(columnName)) {
                int indexOfRow = colNameMap.get(columnName);
                tempArray[indexOfRow] = rowValue;

            }
            else {
                try {
                    throw new IllegalArgumentException("Column doesn't exist!");
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
                return false;
            }

        }
            ArrayList finalRow = new ArrayList(Arrays.asList(tempArray));
            table.add(finalRow);
            DBDriver.database.getRowLocks(tableName).put(table.size() - 1, new ReentrantReadWriteLock(true));

        return true;
    }

    public void deleteRow(int index){
        table.remove(index);
    }

    public ArrayList getRow(int index) {
        return table.get(index);
    }

    public int getNumberOfRows(){
        return table.size();
    }

    public void putRow(ArrayList row, int rowIndex){
        table.set(rowIndex, row);
    }

    public ArrayList getColumnByIndex(int index) {
        ArrayList column = new ArrayList();
        for (ArrayList row : table) {
            column.add(row.get(index));
        }
        return column;
    }

    public ArrayList getColumnByName(String columnName) {
        ArrayList column = new ArrayList();
        for (ArrayList row : table) {
            if (colNameMap.containsKey(columnName)) {
                column.add(row.get(colNameMap.get(columnName)));
            } else {
                throw new NullPointerException("Column doesn't exist!");
            }
        }
        return column;
    }

    public ArrayList getColumnByBTree(BTree btree, Object value, String columnName){
        ArrayList column = new ArrayList();
        ArrayList<ArrayList> rows = (ArrayList) btree.get((Comparable) value);
        if(rows.size() > 1){
            for(ArrayList row : rows){
                if (colNameMap.containsKey(columnName)) {
                    column.add(row.get(colNameMap.get(columnName)));
                }
            }
        }
        else {
            if (colNameMap.containsKey(columnName)) {
                column.add(rows.get(colNameMap.get(columnName)));
            }
        }
        return column;
    }

    public void addColumnNames(String columnName) {
        colNameMap.put(columnName, columnCount);
        columnCount++;
    }

    public void putColumnByIndex(ArrayList column, int indexOfColumn) {
        for (int i = 0; i < column.size(); i++) {
            getRow(i).set(indexOfColumn, column.get(i));
        }
    }

    public void putColumnByName(ArrayList column, String columnName) {
        try {
           // DBDriver.database.getColumnLocks(tableName).get(columnName).writeLock().lock();
            int columnIndex = colNameMap.get(columnName);
            for (int i = 0; i < column.size(); i++) {
                try {
                 //   DBDriver.database.getRowLocks(tableName).get(i).writeLock().lock();
                    getRow(i).set(columnIndex, column.get(i));
                } finally {
                 //   DBDriver.database.getColumnLocks(tableName).get(columnName).writeLock().unlock();
                }

            }
        } finally {
           // DBDriver.database.getColumnLocks(tableName).get(columnName).writeLock().unlock();
        }

    }

    public void reorderTable(ArrayList<Integer> indexes) {

        List<ArrayList> temp = Arrays.asList(new ArrayList[table.size()]);


        for (int i=0; i<table.size(); i++)
            if(i != indexes.size())
                temp.set(i, table.get(indexes.get(i)));

        for (int i=0; i<table.size(); i++)
        {
            if(i != indexes.size())
                table.set(i, temp.get(i));
            //indexes.set(i, i);

        }

    }

    public int getColumnIndex(String columnName ){
        return colNameMap.get(columnName);
    }



    public void printWholeTable(){
        System.out.println("----------------------------------------------------------");
        for(int r = 0; r < table.size(); r++){
            for(int c = 0; c < table.get(r).size(); c++){
                Object element = table.get(r).get(c);
                if(element !=  null) {
                    System.out.print(table.get(r).get(c) + " | ");
                    }
                    else{
                       System.out.print( " | ");
                }

            }
            System.out.println();
        }
        System.out.println("-----------------------------------------------------------");

    }

    public void printWholeTable2() {
        final Object[][] table = new Object[size() + 2][];
        table[0] = this.columnNames.toArray();
        table[1] = new ArrayList<String>(Collections.nCopies(columnNames.size(), "----")).toArray();
        for(int i = 0; i < size(); i++) {
            ArrayList row = this.table.get(i);
            table[i + 2] = row.toArray();
        }
        String format = "";
        for(int i = 0; i < columnNames.size(); i++){
            format += "%13s";
        }
        format += "\n";

        for (final Object[] row : table) {
            System.out.format(format, row);
        }
        System.out.println("printed!");
    }

    public void toggleAllRowLocks(boolean toggle, String lockType){
        HashMap<Integer, ReentrantReadWriteLock> rowLockMap = DBDriver.database.getRowLocks(tableName);
        for(Integer index : rowLockMap.keySet()){
            if(toggle) {
                if(lockType.equals("write")){
                    rowLockMap.get(index).writeLock().lock();
                } else if(lockType.equals("read")) rowLockMap.get(index).readLock().lock();
            } else{
                if(lockType.equals("write")){
                    rowLockMap.get(index).writeLock().unlock();
                } else if(lockType.equals("read")) rowLockMap.get(index).readLock().unlock();
            }
        }
    }

    public void toggleAllColumnLocks(boolean toggle, String lockType){
        HashMap<String, ReentrantReadWriteLock> rowLockMap = DBDriver.database.getColumnLocks(tableName);
        for(String columnName : rowLockMap.keySet()){
            if(toggle) {
                if(lockType.equals("write")){
                    rowLockMap.get(columnName).writeLock().lock();
                } else if(lockType.equals("read")) rowLockMap.get(columnName).readLock().lock();
            } else{
                if(lockType.equals("write")){
                    rowLockMap.get(columnName).writeLock().unlock();
                } else if(lockType.equals("read")) rowLockMap.get(columnName).readLock().unlock();
            }
        }
    }


}



