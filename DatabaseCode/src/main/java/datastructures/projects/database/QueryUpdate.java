package datastructures.projects.database;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.*;
import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

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


    public QueryUpdate(UpdateQuery result) throws JSQLParserException {
        this.result = result;
        this.database = DBDriver.database;
        this.tableName = result.getTableName();
        this.columnValuePairs = result.getColumnValuePairs();
        this.table = this.database.getTable(result.getTableName());
        this.ctq = (QueryCreateTable) this.database.getInfoMap().get(result.getTableName()).get("tableDescription");
        this.tableInfo = ctq.getTableInfo();
        setColumnTypes();

    }

    public QueryCreateTable getCtq() {
        return ctq;
    }

    public void setColumnTypes(){
        this.intMap = ctq.getIntMap();
        this.booleanMap = ctq.getBooleanMap();
        this.doubleMap = ctq.getDoubleMap();
    }
    public boolean isIndexed(String columnName){
        HashMap<String, BTree>  mainMap = database.getBtreeMap().get(result.getTableName());
        if(mainMap.containsKey(columnName)){
            return true;
        }
        return false;
    }

    public boolean update() {
        if (result.getWhereCondition() == null) {
            if(WHEREisNull());
            return true;
        }
        else {
            WHEREisNotNull();
            return true;
        }


    }

    private boolean isLess(Comparable input, Comparable columnValue) {
        return input.compareTo(columnValue) < 0;
    }

    private boolean isEqual(Comparable input, Comparable columnValue) {
        return input.compareTo(columnValue) == 0;
    }

    public void WHEREisNotNull(){ //based of the assignment - only one clause in WHERE
        String[] columnNameArray = new String[columnValuePairs.length];
        HashMap<String, Boolean> uniqueMap = new HashMap<String, Boolean>();

        for(int i = 0; i < tableInfo.length; i++){
            uniqueMap.put(tableInfo[i].getColumnName(), tableInfo[i].isUnique());
            if (table.getPrimaryColumnName().containsKey(tableInfo[i].getColumnName())){
                uniqueMap.put(tableInfo[i].getColumnName(), true);
            }
        }

        Condition root = result.getWhereCondition();
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

        if (!operator.matches("=|>|<|<=|>=|<>")){
            try {
                throw new IllegalArgumentException("Illegal WHERE condition operator!");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < columnValuePairs.length; i++){

            columnNameArray[i] = columnValuePairs[i].getColumnID().getColumnName();
            Object[] tempColumn = new Object[table.getNumberOfRows()];
            ArrayList mainColumn = table.getColumnByName(columnNameArray[i]);
            for (int j = 0; j < tempColumn.length; j++){
                tempColumn[j] = mainColumn.get(j);
            }

            if(!uniqueMap.get(columnNameArray[i])) {

                ArrayList column = table.getColumnByName(id.getColumnName());
                for(int j = 0; j < column.size(); j++){
                    Boolean yes = null;
                    switch(operator) {
                        case "=":
                            yes = column.get(j) != null && isEqual((Comparable) column.get(j), (Comparable) value);
                            break;
                        case "<":
                            yes = column.get(j) != null && isLess((Comparable) column.get(j), (Comparable) value);
                            break;
                        case ">":
                            yes = column.get(j) != null && isLess((Comparable) value, (Comparable) column.get(j));
                            break;
                        case "<>":
                            yes = column.get(j) != null && (!isEqual((Comparable) column.get(j), (Comparable) value));
                            break;
                        case ">=":
                            yes = column.get(j) != null && (isLess((Comparable) value, (Comparable) column.get(j)) || isEqual((Comparable) column.get(j), (Comparable) value));
                            break;
                        case "<=":
                            yes = column.get(j) != null && (isLess((Comparable) column.get(j), (Comparable) value) || isEqual((Comparable) column.get(j), (Comparable) value));
                            break;
                    }
                    if (yes){
                        Object realValue = setTheType(columnValuePairs[i].getValue(), columnNameArray[i]);
                        mainColumn.set(j, realValue);
                    }
                }
                table.putColumnByName(mainColumn, columnValuePairs[i].getColumnID().getColumnName());

            } else{
                try {
                    throw new IllegalArgumentException("Can't UPDATE duplicates in a UNIQUE " + columnValuePairs[i].getColumnID().getColumnName() + " column!");
                } catch(IllegalArgumentException e){
                    e.printStackTrace();
                }

            }


        }

    }

    public boolean WHEREisNull(){

            String[] columnNameArray = new String[columnValuePairs.length];
            HashMap<String, Boolean> uniqueMap = new HashMap<String, Boolean>();

            for(int i = 0; i < tableInfo.length; i++){
                uniqueMap.put(tableInfo[i].getColumnName(), tableInfo[i].isUnique());
                if (table.getPrimaryColumnName().containsKey(tableInfo[i].getColumnName())){
                    uniqueMap.put(tableInfo[i].getColumnName(), true);
                }
            }


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
