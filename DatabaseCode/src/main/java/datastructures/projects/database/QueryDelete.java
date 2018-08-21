package datastructures.projects.database;


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.*;
import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;
import java.util.HashMap;


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

    public boolean delete(){
        if(result.getWhereCondition() == null){
            while (table.getNumberOfRows() != 0) {
                table.deleteRow(0);
            }
        }
        else{
            return WHEREconditions();
        }
        return true;
    }

    public boolean WHEREconditions(){
        Condition root = result.getWhereCondition();
        if(root.getLeftOperand().getClass().getSimpleName().equals("ColumnID")){ //i.e. there's just one operator in query
            return oneOperator(root);
        }
        else {
            inOrder(root);
        }
        return true;
    }
    private boolean inOrder(Condition condition)
    {
        if (condition.getOperator().toString().equals("AND")){
            return (inOrder((Condition) condition.getLeftOperand()) && (inOrder((Condition) condition.getRightOperand())));
        } else if (condition.getOperator().toString().equals("OR")){
            return (inOrder((Condition) condition.getLeftOperand()) || (inOrder((Condition) condition.getRightOperand())));
        }
        else {
            return oneOperator(condition);
        }

    }

    public boolean oneOperator(Condition root) {
        ColumnID id = (ColumnID) root.getLeftOperand();
        String operator = root.getOperator().toString();
        Object value = root.getRightOperand();
        /*
        if (intMap.containsKey(id.getColumnName())){
            value = Integer.parseInt(value.toString());
        }
        else if(doubleMap.containsKey(id.getColumnName())){
            value = Double.parseDouble(value.toString());
        }
        else if(booleanMap.containsKey(id.getColumnName())){
            value = Boolean.parseBoolean(value.toString());
        } */
        if (!table.getColNameMap().containsKey(id.getColumnName())) {
            return false;
        }
        if (!operator.matches("=|>|<|<=|>=|<>")) {
            try {
                throw new IllegalArgumentException("Illegal WHERE condition operator!");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        ArrayList column = table.getColumnByName(id.getColumnName());

        for (int i = column.size() - 1; i >= 0; i--) {
            if (column.get(i) != null) {
                String stringValue = "";
                switch (operator) {
                    case "=":
                        if (column.get(i).toString().equals(value)) {
                            table.deleteRow(i);
                        }
                        break;
                    case "<":
                        stringValue = String.valueOf(column.get(i));
                        if (isLess((Comparable) stringValue, (Comparable) value)) {
                            table.deleteRow(i);
                        }
                        break;
                    case ">":
                        stringValue = String.valueOf(column.get(i));
                        if (isLess((Comparable) value, (Comparable) stringValue)) {
                            table.deleteRow(i);
                        }
                        break;
                    case "<>":
                        if (!column.get(i).toString().equals(value)) {
                            table.deleteRow(i);
                        }
                        break;
                    case ">=":
                        stringValue = String.valueOf(column.get(i));
                        if (isLess((Comparable) value, (Comparable) stringValue) || stringValue.equals(value)) {
                            table.deleteRow(i);
                        }
                        break;
                    case "<=":
                        stringValue = String.valueOf(column.get(i));
                        if (isLess((Comparable) stringValue, (Comparable) value) || stringValue.equals(value)) {
                            table.deleteRow(i);
                        }
                        break;

                }
            }

        }
        return true;
    }


    private boolean isLess(Comparable input, Comparable columnValue) {
        return input.compareTo(columnValue) < 0;
    }

    private boolean isEqual(Comparable input, Comparable columnValue) {
        return input.compareTo(columnValue) == 0;
    }

}
