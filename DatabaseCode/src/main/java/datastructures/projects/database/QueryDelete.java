package datastructures.projects.database;


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.*;
import net.sf.jsqlparser.JSQLParserException;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class QueryDelete {
    private ArrayListTable table;
    private DeleteQuery result;
    private String tableName;
    private ColumnDescription[] tableInfo;
    private QueryCreateTable ctq;
    private ResultSet resultSet;
    private HashMap<String, Boolean> doubleMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> intMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> booleanMap = new HashMap<String, Boolean>();
    private HashMap<String, ArrayList> indexedColumns = new HashMap<>();



    public QueryDelete(DeleteQuery result) throws JSQLParserException {
        this.result = result;
        this.tableName = result.getTableName();
        this.table = DBDriver.database.getTable(result.getTableName());
        if(table == null) return;
        this.ctq = (QueryCreateTable) DBDriver.database.getInfoMap().get(result.getTableName()).get("tableDescription");
        this.tableInfo = ctq.getTableInfo();
        this.resultSet = new ResultSet(result.getQueryString());
        setColumnTypes();
        //delete();

    }

    public void setColumnTypes(){
        this.intMap = ctq.getIntMap();
        this.booleanMap = ctq.getBooleanMap();
        this.doubleMap = ctq.getDoubleMap();
        for (String columnName : table.getColumnNames()) {
            if (isIndexed(columnName)) {
                indexedColumns.put(columnName, table.getColumnByName(columnName));
            }
        }
    }

    public QueryCreateTable getCtq() {
        return ctq;
    }

    public ArrayListTable table() {
        return table;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    private void emptyBtree(){
        for (String columnName : table.getColumnNames()) {
            if (isIndexed(columnName)) {
                BTree btree = DBDriver.database.getBtreeMap().get(result.getTableName()).get(columnName);
                ArrayList column = table.getColumnByName(columnName);
                for (Object o : column) {
                    btree.put((Comparable) o, null);
                }
            }
        }

    }


    public boolean delete(){
        try {

            if (result.getWhereCondition() == null) {
                try {
                    lockAllRows();
                    table.toggleAllColumnLocks(true, "write");
                    toggleBtreeLocks(true);
                    emptyBtree();
                    while (table.getNumberOfRows() != 0) {
                        table.deleteRow(0);
                    }
                    resultSet.setColumns(table.getColumnNames());
                    resultSet.setTable(table.getTable());
                } finally {
                    unlockAllRowsAndDeleteLocks();
                    table.toggleAllColumnLocks(false, "write");
                    toggleBtreeLocks(false);
                }
            } else {

                try {
                    table.toggleAllColumnLocks(true, "write");
                    boolean success = WHEREconditions();
                    resultSet.setColumns(table.getColumnNames());
                    resultSet.setTable(table.getTable());
                    return success;
                } catch (Exception e){
                    e.printStackTrace();
                } finally {
                    table.toggleAllColumnLocks(false, "write");
                }

            }

            return true;
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }

    }

    public boolean WHEREconditions(){


        Condition root = result.getWhereCondition();
        HashMap<Integer, ReentrantReadWriteLock> lockMap = DBDriver.database.getRowLocks(tableName);
        for(int i = table.size() - 1; i >= 0; i--){
            ArrayList row = table.getRow(i);
            boolean wannaDeleteThisRow;
            if(root.getLeftOperand().getClass().getSimpleName().equals("ColumnID")){ //i.e. there's just one operator in query
               wannaDeleteThisRow = oneOperator(root, row);
            }
            else {
                wannaDeleteThisRow = inOrder(root, row);
            }
            if(wannaDeleteThisRow){
                if(resultSet.getQueryResult() == null)
                    return false;
                try {
                    lockMap.get(i).writeLock().lock();
                    toggleBtreeLocks(true);
                    for(String columnName: indexedColumns.keySet()){
                        BTree btree = DBDriver.database.getBtreeMap().get(result.getTableName()).get(columnName);
                        btree.put((Comparable) indexedColumns.get(columnName).get(i), null);
                    }
                    table.deleteRow(i);
                } finally {
                    lockMap.remove(i).writeLock().unlock();
                    toggleBtreeLocks(false);
                }

            }
        }
        int x = 0;
        HashMap<Integer, ReentrantReadWriteLock> newMap = new HashMap<>();
        for (ReentrantReadWriteLock lock : lockMap.values()) {
            newMap.put(x, lock); //gotta make sure row indexes are in sync with the table
            x++;
        }
        DBDriver.database.getInfoMap().get(tableName).put("rowLocks", newMap);
        return true;
    }
    private boolean inOrder(Condition condition, ArrayList row)
    {
        if (condition.getOperator().toString().equals("AND")){
            return (inOrder((Condition) condition.getLeftOperand(), row) && (inOrder((Condition) condition.getRightOperand(), row)));
        } else if (condition.getOperator().toString().equals("OR")){
            return (inOrder((Condition) condition.getLeftOperand(), row) || (inOrder((Condition) condition.getRightOperand(), row)));
        }
        else {
            return oneOperator(condition, row);
        }

    }

    public boolean oneOperator(Condition root, ArrayList row){
        ColumnID id = (ColumnID)root.getLeftOperand();
        String operator = root.getOperator().toString();
        Object value = root.getRightOperand();
        try {
            if (value.equals("NULL")) value = null;

            else if (intMap.containsKey(id.getColumnName())) {
                value = Integer.parseInt(value.toString());
            } else if (doubleMap.containsKey(id.getColumnName())) {
                value = Double.parseDouble(value.toString());
            } else if (booleanMap.containsKey(id.getColumnName())) {
                value = Boolean.parseBoolean(value.toString());
            }
        } catch (Exception e){
            throw new IllegalArgumentException(value + " is a wrong type for " + id.getColumnName());
        }
        Comparable rowValue;
        if(row.get(findIndex(root)) == null) return false;
        else {
            rowValue = (Comparable) row.get(findIndex(root));
        }
        switch (operator) {
            case "=":
              return rowValue.equals(value);
            case "<":
                return isLess(rowValue, (Comparable) value);
            case ">":
                return isLess((Comparable) value, rowValue);
            case "<>":
                return !rowValue.equals(value);
            case ">=":
                return isLess((Comparable) value, rowValue) || rowValue.equals(value);
            case "<=":
                return isLess(rowValue, (Comparable) value) || rowValue.equals(value);
            default:
                try {
                    throw new IllegalArgumentException("Illegal WHERE condition operator!");
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                    resultSet.setQueryResult(null);
                }
                break;
        }


        return true;
    }

    private int findIndex(Condition condition){
        String columnName = condition.getLeftOperand().toString();
        int index;
        for (int i = 0; i < tableInfo.length; i++){
            if (columnName.equals(tableInfo[i].getColumnName())){
                index = i;
                return index;
            }
        }

        return -1;
    }

    private boolean isLess(Comparable input, Comparable columnValue) {
        return input.compareTo(columnValue) < 0;
    }

    private boolean isEqual(Comparable input, Comparable columnValue) {
        return input.compareTo(columnValue) == 0;
    }

    private void lockAllRows(){
        for(int i = 0; i < table.size(); i++){
            DBDriver.database.getRowLocks(tableName).get(i).writeLock().lock();
        }
    }
    private void unlockAllRowsAndDeleteLocks(){
        for(int i = 0; i < table.size(); i++){
            DBDriver.database.getRowLocks(tableName).remove(i).writeLock().unlock();
        }
    }

    public boolean isIndexed(String columnName){
        HashMap<String, BTree>  mainMap = DBDriver.database.getBtreeMap().get(result.getTableName());
        if(mainMap.containsKey(columnName)){
            return true;
        }
        return false;
    }
    private void toggleBtreeLocks(boolean toggle){
        for(String indexedColumn : indexedColumns.keySet()) {
            if (toggle) DBDriver.database.getBtreeLocks(tableName).get(indexedColumn).writeLock().lock();
            else DBDriver.database.getBtreeLocks(tableName).get(indexedColumn).writeLock().unlock();
        }
    }


}
