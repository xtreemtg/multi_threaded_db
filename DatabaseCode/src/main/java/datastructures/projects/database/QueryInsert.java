package datastructures.projects.database;


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnValuePair;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.InsertQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;
import net.sf.jsqlparser.JSQLParserException;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class QueryInsert {
    private ArrayListTable table;
    private InsertQuery result;
    private String tableName;
    private ColumnValuePair[] columnValuePairs;
    private ColumnDescription[] tableInfo;
    private QueryCreateTable ctq;
    private Database database;
    private HashMap<String, Boolean> doubleMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> intMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> booleanMap = new HashMap<String, Boolean>();


    public QueryInsert(InsertQuery result) throws JSQLParserException {
        this.result = result;
        this.tableName = result.getTableName();
        this.columnValuePairs = result.getColumnValuePairs();
        this.database = DBDriver.database;
        this.table = database.getTable(result.getTableName());
        if(this.table == null){
            return;
        }
        this.ctq = (QueryCreateTable) database.getInfoMap().get(result.getTableName()).get("tableDescription");
        this.tableInfo = ctq.getTableInfo();
        setColumnTypes();


    }
/*
    @Override
    public String toString() {
        String result = String.valueOf(insertRows());
        return result;
    }*/

    public void setColumnTypes(){
       this.intMap = ctq.getIntMap();
       this.booleanMap = ctq.getBooleanMap();
       this.doubleMap = ctq.getDoubleMap();
    }

    public QueryCreateTable getCtq() {
        return ctq;
    }

    public ColumnValuePair[] getColumnValuePairs() {
        return columnValuePairs;
    }

    public String getTableName() {
        return tableName;
    }

    public ArrayListTable getTable() {
        return table;
    }

    public boolean insertRows() {
        ArrayList rowOfValues = new ArrayList();
        ArrayList<String> rowOfColumnNames = new ArrayList();
        for (ColumnValuePair cvp : columnValuePairs) {
            String stringValue = cvp.getValue();

            if(!setConstraints(stringValue, cvp)){
                return false;
            }

            Object realValue = setTheType(stringValue, cvp.getColumnID().getColumnName());
            String columnName = cvp.getColumnID().getColumnName();



            rowOfValues.add(realValue);
            rowOfColumnNames.add(columnName);

        }


        if(!table.addRow(rowOfValues, rowOfColumnNames)){
            return false;
        }

        for (int i = 0; i < rowOfColumnNames.size(); i++){
            HashMap<String, BTree>  mainMap = database.getBtreeMap().get(result.getTableName());
            boolean isIndexed = mainMap.containsKey(rowOfColumnNames.get(i));
            if(isIndexed){
                BTree btree = mainMap.get(rowOfColumnNames.get(i));
                int columnIndex = table.getColumnIndex(rowOfColumnNames.get(i));
                ArrayList row = table.getRow(table.getNumberOfRows() - 1);
                Object key = row.get(columnIndex);
                if(key != null){
                    ArrayList<ArrayList> listOfValues = (ArrayList<ArrayList>)btree.get((Comparable) key);
                    if(listOfValues != null){
                        listOfValues.add(row);
                    }
                    else{
                        ArrayList<ArrayList> newListOfValues = new ArrayList<ArrayList>();
                        newListOfValues.add(row);
                        btree.put((Comparable) key, newListOfValues);
                    }
                }
            }
        }

        setDefaultColumns();
        if(!checkNotNullColumns() || !checkUniqueColumns()){
            return false;
        }


        return true;

    }

    public boolean setConstraints(String value, ColumnValuePair cvp){
        for (ColumnDescription description : tableInfo){
            if (cvp.getColumnID().getColumnName().equals(description.getColumnName())){
                if(description.getVarCharLength() != 0 && description.getColumnType().toString().equals("VARCHAR")){
                    int stringLength = description.getVarCharLength();
                    if (value.length() > stringLength){
                        try {
                            throw new IllegalArgumentException("VARCHAR can't be longer than " + stringLength + " Characters!");
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        }
                        table.deleteRow(table.size() - 1);
                        return false;
                    }
                }
                else if (description.getColumnType().toString().equals("DECIMAL") &&
                        (description.getWholeNumberLength() != 0 || description.getFractionLength() !=0)){
                    int fractionLength = description.getFractionLength();
                    int wholeNumLength = description.getWholeNumberLength();
                    int actualNumLength = value.indexOf(".");
                    int actualFracLength = value.length() - actualNumLength - 1;
                    if(actualFracLength > fractionLength || actualNumLength > wholeNumLength){
                        try {
                            throw new IllegalArgumentException("Whole Number Length can't be longer than " + wholeNumLength + " digit(s)" +
                                    " and Fraction Length can't be longer than " + fractionLength + " digit(s)!!");
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        }
                        table.deleteRow(table.size() - 1);
                        return false;
                    }
                }
            }
        }
        return true;

    }

    public void setDefaultColumns() {
        HashMap<String, Boolean> defaultMap = new HashMap<String, Boolean>();
        HashMap<String, String> defaultValueMap = new HashMap<String, String>();
        for (int i = 0; i < tableInfo.length; i++) {
            defaultMap.put(tableInfo[i].getColumnName(), tableInfo[i].getHasDefault());
            defaultValueMap.put(tableInfo[i].getColumnName(), tableInfo[i].getDefaultValue());


            if (tableInfo[i].getHasDefault()) {
                ArrayList column = table.getColumnByName(tableInfo[i].getColumnName());
                if (column.get(column.size() - 1) == null) {
                    String defaultValue = defaultValueMap.get(tableInfo[i].getColumnName());
                    Object realDefaultValue = setTheType(defaultValue,tableInfo[i].getColumnName());
                    column.set(column.size() - 1, realDefaultValue);
                }

                table.putColumnByName(column, tableInfo[i].getColumnName());

            }

        }
    }



    public boolean checkNotNullColumns() {
        for (ColumnDescription description : tableInfo) {
            ArrayList<String> column = table.getColumnByName(description.getColumnName());
            if (description.isNotNull() || (table.getPrimaryColumnName().containsKey(description.getColumnName()))) {
                if (column.get(column.size() - 1) == null) {
                    try {
                        throw new IllegalArgumentException("NotNULL " + description.getColumnName() + " column can't contain a null!");
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                    table.deleteRow(table.size() - 1);
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkUniqueColumns() {
        HashSet uniqueSet = new HashSet();
        for (int i = 0; i < tableInfo.length; i++) {
            ArrayList column = table.getColumnByName(tableInfo[i].getColumnName());
            if (tableInfo[i].isUnique() || (table.getPrimaryColumnName().containsKey(tableInfo[i].getColumnName()))) {
                for (Object columnValue : column)
                    if (!uniqueSet.add(columnValue)) {
                        if (columnValue != null) {
                            try {
                                throw new IllegalArgumentException("Can't have duplicates in UNIQUE " + tableInfo[i].getColumnName() + " column!");
                            } catch (IllegalArgumentException e) {
                                e.printStackTrace();
                            }
                            table.deleteRow(table.size() - 1);
                            return false;
                        }
                    }
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
                    throw new IllegalArgumentException("Can only add 'true' or false' to BOOLEAN column!");
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            } else if (intMap.containsKey(columnName)) {
                try {
                    throw new IllegalArgumentException("Can only add ints to INT column!");
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }

            } else if (doubleMap.containsKey(columnName)) {
                try {
                    throw new IllegalArgumentException("Can only add decimals to DECIMAL column!");
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

    public ArrayListTable table(){
        return this.table;
    }
}






