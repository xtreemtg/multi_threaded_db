package datastructures.projects.database;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.*;
import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryUpdate {

    private ArrayListTable table;
    private UpdateQuery result;
    private String tableName;
    private ColumnDescription[] tableInfo;
    private ColumnValuePair[] columnValuePairs;
    private QueryCreateTable ctq;
    private Database database;
    private HashMap<String, Boolean> doubleMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> intMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> booleanMap = new HashMap<String, Boolean>();
    private HashSet<String> columnsToLock = new HashSet<>();
    private ResultSet resultSet;
    HashMap<String, Boolean> uniqueMap = new HashMap<String, Boolean>();


    public QueryUpdate(UpdateQuery result) throws JSQLParserException {
        this.result = result;
        //this.columnNames = result.getSelectedColumnNames();
        this.database = DBDriver.database;
        this.tableName = result.getTableName();
        this.columnValuePairs = result.getColumnValuePairs();
        this.table = this.database.getTable(result.getTableName());
        this.ctq = (QueryCreateTable) this.database.getInfoMap().get(result.getTableName()).get("tableDescription");
        this.tableInfo = ctq.getTableInfo();
        this.resultSet = new ResultSet(result.getQueryString());
        setColumnTypes();

    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public QueryCreateTable getCtq() {
        return ctq;
    }

    public void setColumnTypes(){
        this.intMap = ctq.getIntMap();
        this.booleanMap = ctq.getBooleanMap();
        this.doubleMap = ctq.getDoubleMap();
        for(int i = 0; i  < tableInfo.length; i++){
            uniqueMap.put(tableInfo[i].getColumnName(), tableInfo[i].isUnique());
            if (table.getPrimaryColumnName().containsKey(tableInfo[i].getColumnName())){
                uniqueMap.put(tableInfo[i].getColumnName(), true);
            }
        }
    }
    public boolean isIndexed(String columnName){
        HashMap<String, BTree>  mainMap = database.getBtreeMap().get(result.getTableName());
        if(mainMap.containsKey(columnName)){
            return true;
        }
        return false;
    }

    public boolean update() {

            for(int i = 0; i < columnValuePairs.length; i++) {
                columnsToLock.add(columnValuePairs[i].getColumnID().getColumnName());
            }
            if (result.getWhereCondition() == null) {
                try {
                    table.toggleAllRowLocks(true, "write");
                    toggleLockColumns(true);
                    return WHEREisNull();
                } catch (Exception e){
                    e.printStackTrace();
                    return false;
                } finally {
                    table.toggleAllRowLocks(false, "write");
                    toggleLockColumns(false);
                }
            } else {
                getColumnsToLock(result.getWhereCondition());
                try {
                    toggleLockColumns(true);
                    return WHEREisNotNull();
                } catch (Exception e){
                    e.printStackTrace();
                    return false;
                } finally {
                    toggleLockColumns(false);
                }

            }


    }

    private void getColumnsToLock(Condition condition){
        if (condition.getOperator().toString().equals("AND") || condition.getOperator().toString().equals("OR")) {
            getColumnsToLock((Condition) condition.getLeftOperand());
            getColumnsToLock((Condition) condition.getRightOperand());
        } else {
            columnsToLock.add((condition.getLeftOperand().toString()));
        }
    }
    private void toggleLockColumns(boolean toggle){
        HashMap<String, ReentrantReadWriteLock> columnLockMap = DBDriver.database.getColumnLocks(tableName);
        for(String name : columnsToLock) {
            if(toggle) columnLockMap.get(name).writeLock().lock();
            else columnLockMap.get(name).writeLock().unlock();
        }
    }

    public boolean WHEREisNotNull(){ //based of the assignment - only one clause in WHERE

        Condition root = result.getWhereCondition();
        HashMap<Integer, ReentrantReadWriteLock> rowLockMap = DBDriver.database.getRowLocks(tableName);
        for(int j = 0; j < table.size(); j++) {
            ArrayList row = table.getRow(j);

            boolean weWannaUpdateThisRow;
            if (root.getLeftOperand().getClass().getSimpleName().equals("ColumnID")) { //i.e. there's just one operator in query
                weWannaUpdateThisRow = oneOperator(root, row);
            } else {
                weWannaUpdateThisRow = inOrder(root, row);
            }
            if(weWannaUpdateThisRow){
                try {
                    rowLockMap.get(j).writeLock().lock();
                    for (int i = 0; i < columnValuePairs.length; i++) {
                        String columnName = columnValuePairs[i].getColumnID().getColumnName();
                        if (uniqueMap.get(columnName) && table.getNumberOfRows() > 1) {
                            try {
                                throw new IllegalArgumentException("Can't UPDATE duplicates in a UNIQUE " + columnValuePairs[i].getColumnID().getColumnName() + " column!");
                            } catch (IllegalArgumentException e) {
                                e.printStackTrace();
                            }
                            return false;
                        }
                    }

                    for (int i = 0; i < columnValuePairs.length; i++) {
                        String columnName = columnValuePairs[i].getColumnID().getColumnName();
                        int index = table.getColNameMap().get(columnName);
                        row.set(index, setTheType(columnValuePairs[i].getValue(), columnName));

                    }
                } catch (Exception e){
                    e.printStackTrace();
                    return false;
                } finally {
                    rowLockMap.get(j).writeLock().unlock();
                }
            }

        }
        resultSet.setColumns(this.table.getColumnNames());
        resultSet.setTable(this.table.getTable());
        //resultSet.printWholeTable2();
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
    public boolean oneOperator(Condition root, ArrayList row)  {
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

        if(operator.equals("=")){
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
                return false;
            }
        }
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

    public boolean WHEREisNull(){

            String[] columnNameArray = new String[columnValuePairs.length];

            for (int i = 0; i < columnValuePairs.length; i++){

                columnNameArray[i] = columnValuePairs[i].getColumnID().getColumnName();
                Object[] tempColumn = new Object[table.getNumberOfRows()];

                if(!uniqueMap.get(columnNameArray[i])) {
                    if (isIndexed(columnValuePairs[i].getColumnID().getColumnName())) {
                        BTree btree = database.getBtreeMap().get(result.getTableName()).get(columnValuePairs[i].getColumnID().getColumnName());
                        ArrayList column = table.getColumnByName(columnValuePairs[i].getColumnID().getColumnName());
                        for (Object element : column) {
                            if (element != null) {
                                ArrayList listOfRows = (ArrayList) btree.get((Comparable) element);
                                listOfRows.clear();
                                btree.put((Comparable) element, null);
                            }
                        }

                        Object realValue = setTheType(columnValuePairs[i].getValue(), columnNameArray[i]);
                        Arrays.fill(tempColumn, realValue);
                        ArrayList finalColumn = new ArrayList(Arrays.asList(tempColumn));
                        table.putColumnByName(finalColumn, columnValuePairs[i].getColumnID().getColumnName());
                        btree.put((Comparable) realValue, table.getTable());

                    }
                    else{
                        Object realValue = setTheType(columnValuePairs[i].getValue(), columnNameArray[i]);
                        Arrays.fill(tempColumn, realValue);
                        ArrayList finalColumn = new ArrayList(Arrays.asList(tempColumn));
                        table.putColumnByName(finalColumn, columnValuePairs[i].getColumnID().getColumnName());
                    }
                    resultSet.setColumns(this.table.getColumnNames());
                    resultSet.setTable(this.table.getTable());
                    //resultSet.printWholeTable2();
                }
                else if (table.getNumberOfRows() <= 1) {
                    if (isIndexed(columnValuePairs[i].getColumnID().getColumnName())) {
                        BTree btree = database.getBtreeMap().get(result.getTableName()).get(columnValuePairs[i].getColumnID().getColumnName());
                        ArrayList column = table.getColumnByName(columnValuePairs[i].getColumnID().getColumnName());
                        for (Object element : column) {
                            if (element != null) {
                                ArrayList listOfRows = (ArrayList) btree.get((Comparable) element);
                                listOfRows.clear();
                                btree.put((Comparable) element, null);
                            }
                        }

                            Object realValue = setTheType(columnValuePairs[i].getValue(), columnNameArray[i]);
                            Arrays.fill(tempColumn, realValue);
                            ArrayList finalColumn = new ArrayList(Arrays.asList(tempColumn));
                            table.putColumnByName(finalColumn, columnValuePairs[i].getColumnID().getColumnName());

                            table.putColumnByName(finalColumn, columnValuePairs[i].getColumnID().getColumnName());
                            btree.put((Comparable) realValue, table.getTable());


                    }
                    else{
                        Object realValue = setTheType(columnValuePairs[i].getValue(), columnNameArray[i]);
                        Arrays.fill(tempColumn, realValue);
                        ArrayList finalColumn = new ArrayList(Arrays.asList(tempColumn));
                        table.putColumnByName(finalColumn, columnValuePairs[i].getColumnID().getColumnName());
                    }
                    resultSet.setColumns(this.table.getColumnNames());
                    resultSet.setTable(this.table.getTable());
                    //resultSet.printWholeTable2();
                }
                else{
                    try {
                        throw new IllegalArgumentException("Can't UPDATE duplicates in a UNIQUE " + columnValuePairs[i].getColumnID().getColumnName() + " column!");
                    } catch(IllegalArgumentException e){
                        e.printStackTrace();
                    }
                    return false;
                }
            }
            return true;

    }

    public Object setTheType(String value, String columnName) {
        boolean isInteger = isNumeric(value);
        boolean isDouble = isDouble(value);

        if (isDouble && value.contains(".")) {

            if (doubleMap.get(columnName)) {
                double newDoubleValue = Double.parseDouble(value);
                return newDoubleValue;
            }
        }

        else if (isInteger) {

            if (intMap.get(columnName)) {
                int newIntValue = Integer.parseInt(value);
                return newIntValue;
            }

        }
        else if (value.equals("true") || value.equals("false")) {

            if (booleanMap.get(columnName)) {
                //
                Boolean newBooleanValue = Boolean.valueOf(value);
                return newBooleanValue;
            }

        }
        else {
            if (booleanMap.containsKey(columnName)) {
                try {
                    throw new IllegalArgumentException("Can only update 'true' or false' to BOOLEAN column!");
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            } else if (intMap.containsKey(columnName)) {
                try {
                    throw new IllegalArgumentException("Can only update ints to INT column!");
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }

            } else if (doubleMap.containsKey(columnName)) {
                try {
                    throw new IllegalArgumentException("Can only update decimals to DECIMAL column!");
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            } else {
                return value;
            }
        }
        return null;
    }




    //https://stackoverflow.com/questions/14206768/how-to-check-if-a-string-is-numeric
    //https://bukkit.org/threads/check-if-string-is-parseable-to-double.96893/
    private boolean isNumeric(String s) {
        boolean amIValid = false;
        try {
            Integer.parseInt(s);
            amIValid = true;
        }
        catch(NumberFormatException e) {

        }
        return amIValid;
    }

    private static boolean isDouble(String s) {
        boolean amIValid = false;
        try {
            Double.parseDouble(s);
            amIValid = true;
        }
        catch(NumberFormatException e){

        }
        return amIValid;
    }

}
