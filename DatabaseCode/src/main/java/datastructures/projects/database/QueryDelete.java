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



    public QueryDelete(DeleteQuery result) throws JSQLParserException {
        this.result = result;
        this.tableName = result.getTableName();
        this.table = DBDriver.database.getTable(result.getTableName());
        this.ctq = (QueryCreateTable) DBDriver.database.getInfoMap().get(result.getTableName()).get("tableDescription");
        this.tableInfo = ctq.getTableInfo();
        this.resultSet = new ResultSet();
        setColumnTypes();
        //delete();

    }

    public void setColumnTypes(){
        this.intMap = ctq.getIntMap();
        this.booleanMap = ctq.getBooleanMap();
        this.doubleMap = ctq.getDoubleMap();
    }

    public QueryCreateTable getCtq() {
        return ctq;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }


    public boolean delete(){
        try {

            if (result.getWhereCondition() == null) {
                try {
                    lockAllRows();
                    table.toggleAllColumnLocks(true, "write");
                    while (table.getNumberOfRows() != 0) {
                        table.deleteRow(0);
                    }
                    resultSet.setColumns(table.getColumnNames());
                    resultSet.setTable(table.getTable());
                } finally {
                    unlockAllRowsAndDeleteLocks();
                    table.toggleAllColumnLocks(false, "write");
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
                try {
                    lockMap.get(i).writeLock().lock();
                    table.deleteRow(i);
                } finally {
                    lockMap.remove(i).writeLock().unlock();
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
        if (intMap.containsKey(id.getColumnName())){
            value = Integer.parseInt(value.toString());
        }
        else if(doubleMap.containsKey(id.getColumnName())){
            value = Double.parseDouble(value.toString());
        }
        else if(booleanMap.containsKey(id.getColumnName())){
            value = Boolean.parseBoolean(value.toString());
        }
        Comparable rowValue;
        if(row.get(findIndex(root)) == null) return false;
        else {
            rowValue = (Comparable) row.get(findIndex(root));
        }
        if(operator.equals("=")){
//            if(isIndexed(id.getColumnName())){
//                BTree btree = database.getBtreeMap().get(result.getFromTableNames()[0]).get(id.getColumnName());
//                ArrayList<ArrayList> listOfRows = (ArrayList<ArrayList>) btree.get((Comparable) value);
//                resultSet.setTable(listOfRows);
//                return true;
//            }
            return rowValue.equals(value);

        }
        else if(operator.equals("<")){
            return isLess(rowValue, (Comparable) value);
        }
        else if(operator.equals(">")){
            return isLess((Comparable) value, rowValue);
        }
        else if(operator.equals("<>")){
            return !rowValue.equals(value);
        }
        else if(operator.equals(">=")){
            return isLess((Comparable) value, rowValue) || rowValue.equals(value);
        }
        else if(operator.equals("<=")){
            return isLess(rowValue, (Comparable) value) || rowValue.equals(value);
        }
        else{
            try {
                throw new IllegalArgumentException("Illegal WHERE condition operator!");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
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


}
