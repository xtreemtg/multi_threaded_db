package datastructures.projects.database;


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class QuerySelect {
    private ArrayListTable table;
    private SelectQuery result;
    private ColumnDescription[] tableInfo;
    private QueryCreateTable ctq;
    private ColumnID[] columnNames;
    private ArrayList<String> actualColumnNames = new ArrayList<>();
    private HashMap<String, Boolean> doubleMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> intMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> booleanMap = new HashMap<String, Boolean>();
    private Database database;
    private ResultSet resultSet;

    public QuerySelect(SelectQuery result) throws JSQLParserException {
        this.result = result;
        this.columnNames = result.getSelectedColumnNames();
        this.database = DBDriver.database;
        this.ctq = (QueryCreateTable) this.database.getInfoMap().get(result.getFromTableNames()[0]).get("tableDescription");
        this.table = this.database.getTable(result.getFromTableNames()[0]);
        this.tableInfo = ctq.getTableInfo();
        this.resultSet = new ResultSet();
        setColumnTypes();
        select();

    }

    public void setColumnTypes(){
        this.intMap = ctq.getIntMap();
        this.booleanMap = ctq.getBooleanMap();
        this.doubleMap = ctq.getDoubleMap();
        for(ColumnID c : columnNames){
            this.actualColumnNames.add(c.getColumnName());
        }
    }

    public QueryCreateTable getCtq() {
        return ctq;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public boolean isIndexed(String columnName){
        HashMap<String, BTree>  mainMap = database.getBtreeMap().get(result.getFromTableNames()[0]);
        if(mainMap.containsKey(columnName)){
            return true;
        }
        return false;
    }

    public void select() {
        if (result.getWhereCondition() == null) {

            if (columnNames[0].getColumnName().equals("*") && result.getFunctions().isEmpty() && result.getOrderBys().length == 0) {
                try {
                    table.toggleAllRowLocks(true, "read");
                    table.toggleAllColumnLocks(true, "read");
                    resultSet.setColumns(this.table.getColumnNames());
                    resultSet.setTable(this.table.getTable());
                    resultSet.setQueryResult(true);
                } catch (Exception e){
                    e.printStackTrace();
                    resultSet.setQueryResult(false);
                } finally {
                    table.toggleAllRowLocks(false, "read");
                    table.toggleAllColumnLocks(false, "read");
                }


            }
            else if (result.getFunctions().isEmpty() && result.getOrderBys().length == 0) {
                try {
                    table.toggleAllRowLocks(true, "read");
                    toggleColumnReadLock(this.actualColumnNames, true);
                    ArrayList<ArrayList> temp = new ArrayList<ArrayList>();
                    ArrayList<String> names = new ArrayList<>();
                    for (int i = 0; i < columnNames.length; i++) {
                        names.add(columnNames[i].getColumnName());
                        temp.add(table.getColumnByName(columnNames[i].getColumnName()));
                    }
                    ArrayList<ArrayList> resultSetColumns = new ArrayList<>();
                    for (int i = 0; i < table.size(); i++) {
                        ArrayList row = new ArrayList<>();
                        row.add(temp.get(0).get(i));
                        resultSetColumns.add(row);
                    }
                    for (int j = 1; j < temp.size(); j++) {
                        if (j > columnNames.length) break;
                        for (int i = 0; i < table.size(); i++) {
                            resultSetColumns.get(i).add(temp.get(j).get(i));
                        }

                    }
                    resultSet.setColumns(names);
                    resultSet.setTable(resultSetColumns);
                    resultSet.setQueryResult(true);

                } finally {
                    table.toggleAllRowLocks(false, "read");
                    toggleColumnReadLock(this.actualColumnNames, false);
                }
            }
            else if(result.getOrderBys().length != 0){
                boolean star = actualColumnNames.get(0).equals("*");
                try {
                    if (star) table.toggleAllColumnLocks(true, "read");
                    else toggleColumnReadLock(actualColumnNames, true);
                    table.toggleAllRowLocks(true, "read");
                    ArrayList<ArrayList> resultSetColumns = new ArrayList<ArrayList>();
                    int x = 0;
                    for (SelectQuery.OrderBy orderBys : result.getOrderBys()) {
                        if (booleanMap.containsKey(orderBys.getColumnID().getColumnName())) {
                            try {
                                throw new IllegalArgumentException("Can't order by booleans!");
                            } catch (IllegalArgumentException e) {
                                e.printStackTrace();
                            }
                        }
                        //if (intMap.containsKey(orderBys.getColumnID().getColumnName())){
                        else {
                            ArrayList column = table.getColumnByName(orderBys.getColumnID().getColumnName());
                            for (int i = column.size() - 1; i >= 0; i--) {

                                //if(column.get(i) != null) {
                                HashMap<Object, Integer> map = new HashMap<>();
                                map.put(column.get(i), i);
                                column.set(i, map);
                                //}
                                //else column.remove(i);

                            }
                            ArrayList<HashMap> sorted = Sorter.insertionSort(table, column, result.getOrderBys(), x, orderBys.isAscending(), orderBys.isDescending());
                            ArrayList sortedIndexes = new ArrayList<>();
                            for (int i = 0; i < sorted.size(); i++) {
                                sortedIndexes.add(sorted.get(i).values().toArray()[0]);
                            }
                            table.reorderTable(sortedIndexes);
                        }
                        x++;
                    }
                    if (star) {
                        resultSetColumns = table.getTable();
                        resultSet.setColumns(this.table.getColumnNames());
                    } else {
                        ArrayList<String> names = new ArrayList<>();
                        for (int i = 0; i < columnNames.length; i++) {
                            names.add(columnNames[i].getColumnName());
                            resultSetColumns.add(table.getColumnByName(columnNames[i].getColumnName()));
                            resultSet.setColumns(names);
                        }
                    }

                    resultSet.setTable(resultSetColumns);
                    resultSet.setQueryResult(true);

                } finally {
                    if (star) table.toggleAllColumnLocks(false, "read");
                    else toggleColumnReadLock(actualColumnNames, false);
                    table.toggleAllRowLocks(false, "read");
                }

            } else{
                  specialSELECTFunctions();
            }


        }

        else{
            ArrayList<ArrayList> resultSetColumns = new ArrayList<>();
            int x = 0;
            Condition root = result.getWhereCondition();
            for(ArrayList row : table.getTable()) {
                boolean weWannaAddThisRow;
                if (root.getLeftOperand().getClass().getSimpleName().equals("ColumnID")) { //i.e. there's just one operator in query
                    weWannaAddThisRow = oneOperator(root, row);
                } else {
                    weWannaAddThisRow = inOrder(root, row);
                }
                if(weWannaAddThisRow){
                    resultSetColumns.add(new ArrayList());
                    if(!columnNames[0].getColumnName().equals("*")) {
                        ArrayList<String> names = new ArrayList<>();
                        for (int i = 0; i < columnNames.length; i++) {
                            names.add(columnNames[i].getColumnName());
                            int index = table.getColNameMap().get(columnNames[i].getColumnName());
                            resultSetColumns.get(x).add(row.get(index));
                        }
                        resultSet.setColumns(names);
                    } else{
                        resultSet.setColumns(this.table.getColumnNames());
                    }
                    x++;
                }

            }
            resultSet.setTable(resultSetColumns);
            resultSet.setQueryResult(true);


        }


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

    public void specialSELECTFunctions(){
        try {
            toggleColumnReadLock(actualColumnNames, true);
            table.toggleAllRowLocks(true, "read");
            ArrayList<Double> resultSetColumns = new ArrayList<Double>();
            if (result.getFunctions().get(0).function.toString().equals("AVG")) {
                if (intMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Integer> column = table.getColumnByName(columnNames[0].getColumnName());
                    double average = getINTAverageOfColumn(column);
                    System.out.println("Average of " + columnNames[0].getColumnName() + " is " + average);
                    //resultSet.getTable().get(0).add(average);
                    resultSet.setQueryResult(average);
                } else if (doubleMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Double> column = table.getColumnByName(columnNames[0].getColumnName());
                    double average = getDECIMALAverageOfColumn(column);
                    System.out.println("Average of " + columnNames[0].getColumnName() + " is " + average);
                    //resultSet.getTable().get(0).add(average);
                    resultSet.setQueryResult(average);
                }
            } else if (result.getFunctions().get(0).function.toString().equals("COUNT")) {
                if (result.getFunctions().get(0).isDistinct) {
                    ArrayList column = table.getColumnByName(columnNames[0].getColumnName());
                    HashSet distinctSet = new HashSet();
                    for (Object element : column) {
                        if (element != null) {
                            distinctSet.add(element);
                        }
                    }
                    int numOfDistinctElements = 0;
                    for (Object element : distinctSet) {
                        if (element != null) {
                            numOfDistinctElements++;
                        }
                    }
                    System.out.println(numOfDistinctElements + " DISTINCT elements in " + columnNames[0].getColumnName() + " column");
                    //resultSet.getTable().get(0).add(numOfDistinctElements);
                    resultSet.setQueryResult(numOfDistinctElements);
                } else {
                    ArrayList column = table.getColumnByName(columnNames[0].getColumnName());
                    int numOfElements = 0;
                    for (Object element : column) {
                        if (element != null) {
                            numOfElements++;
                        }
                    }
                    System.out.println(numOfElements + " elements in " + columnNames[0].getColumnName() + " column");
                    //resultSet.getTable().get(0).add(numOfElements);
                    resultSet.setQueryResult(numOfElements);
                }

            } else if (result.getFunctions().get(0).function.toString().equals("MAX")) {
                if (intMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Integer> column = table.getColumnByName(columnNames[0].getColumnName());
                    int max = getINTMax(column);
                    System.out.println("Max in " + columnNames[0].getColumnName() + " is " + max);
                    //resultSet.getTable().get(0).add(max);
                    resultSet.setQueryResult(max);
                } else if (doubleMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Double> column = table.getColumnByName(columnNames[0].getColumnName());
                    double max = getDECIMALMax(column);
                    System.out.println("Max in " + columnNames[0].getColumnName() + " is " + max);
                    //resultSet.getTable().get(0).add(max);
                    resultSet.setQueryResult(max);
                } else if (booleanMap.containsKey(columnNames[0].getColumnName())) {
                    try {
                        throw new IllegalArgumentException("Can't get MAX of BOOLEAN column!");
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                } else {
                    ArrayList<String> column = table.getColumnByName(columnNames[0].getColumnName());
                    String max = getVARCHARMax(column);
                    System.out.println("Max in " + columnNames[0].getColumnName() + " is " + max);
                    ArrayList<String> rsltsetcol = new ArrayList<String>();
                    //resultSet.getTable().get(0).add(max);
                    resultSet.setQueryResult(max);
                }

            } else if (result.getFunctions().get(0).function.toString().equals("MIN")) {
                if (intMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Integer> column = table.getColumnByName(columnNames[0].getColumnName());
                    int min = getINTMin(column);
                    System.out.println("Min in " + columnNames[0].getColumnName() + " is " + min);
                    //resultSet.getTable().get(0).add(min);
                    resultSet.setQueryResult(min);
                } else if (doubleMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Double> column = table.getColumnByName(columnNames[0].getColumnName());
                    double min = getDECIMALMin(column);
                    System.out.println("Min in " + columnNames[0].getColumnName() + " is " + min);

                    //resultSet.getTable().get(0).add(min);
                    resultSet.setQueryResult(min);
                } else if (booleanMap.containsKey(columnNames[0].getColumnName())) {
                    try {
                        throw new IllegalArgumentException("Can't get MIN of BOOLEAN column!");
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                } else {
                    ArrayList<String> column = table.getColumnByName(columnNames[0].getColumnName());
                    String min = getVARCHARMin(column);
                    System.out.println("Min in " + columnNames[0].getColumnName() + " is " + min);
                    //resultSet.getTable().get(0).add(min);
                    resultSet.setQueryResult(min);
                }

            } else if (result.getFunctions().get(0).function.toString().equals("SUM")) {
                if (intMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Integer> column = table.getColumnByName(columnNames[0].getColumnName());
                    int sum = getINTSumOfColumn(column);
                    System.out.println("Sum of " + columnNames[0].getColumnName() + " is " + sum);

                    //resultSet.getTable().get(0).add(sum);
                    resultSet.setQueryResult(sum);
                } else if (doubleMap.containsKey(columnNames[0].getColumnName())) {
                    ArrayList<Double> column = table.getColumnByName(columnNames[0].getColumnName());
                    double sum = getDECIMALSumOfColumn(column);
                    System.out.println("Sum of " + columnNames[0].getColumnName() + " is " + sum);

                    //resultSet.getTable().get(0).add(sum);
                    resultSet.setQueryResult(sum);
                } else {
                    try {
                        throw new IllegalArgumentException("Can only get SUM of INT and DECIMAL columns!");
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            toggleColumnReadLock(actualColumnNames, false);
            table.toggleAllRowLocks(false, "read");
        }
        resultSet.setTable(this.table.getTable());
        resultSet.setColumns(this.table.getColumnNames());

    }

    private int getINTMax(ArrayList<Integer> column){
        int max = Integer.MIN_VALUE;
        for(int i = 0; i < column.size(); i++){
            if(column.get(i) > max){
                max = column.get(i);
            }
        }
        return max;
    }

    private int getINTMin(ArrayList<Integer> column){
        int min = Integer.MAX_VALUE;
        for(int i = 0; i < column.size(); i++){
            if(column.get(i) < min){
                min = column.get(i);
            }
        }
        return min;
    }


    private double getDECIMALMax(ArrayList<Double> column){
        double max = Double.MIN_VALUE;
        for(int i = 0; i < column.size(); i++){
            if((column.get(i) > max)){
                max = column.get(i);
            }
        }
        return max;
    }

    private double getDECIMALMin(ArrayList<Double> column){
        double min = Integer.MAX_VALUE;
        for(int i = 0; i < column.size(); i++){
            if(column.get(i) < min){
                min = column.get(i);
            }
        }
        return min;
    }


    private String getVARCHARMax(ArrayList<String> column){
        String max = "";
        for(int i = 0; i < column.size(); i++){
            if(max.compareTo(column.get(i)) < 0){
                max = column.get(i);
            }
        }
        return max;
    }

    private String getVARCHARMin(ArrayList<String> column){
        String min = column.get(0);
        for(int i = 0; i < column.size(); i++){
            if(min.compareTo(column.get(i)) > 0) {
                min = column.get(i);
            }
        }
        return min;
    }

    private double getINTAverageOfColumn(ArrayList<Integer> column){
        Integer sum = 0;
        if(column.isEmpty()){
            try {
                throw new IllegalArgumentException("Can't get AVG of empty column!");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        else{
            for (Integer number : column){
                if(number != null) {
                    sum += number;
                }
            }
            return  (sum.doubleValue() / column.size());
        }
        return 0;
    }

    private double getDECIMALAverageOfColumn(ArrayList<Double> column){
        double sum = 0;
        if(column.isEmpty()){
            try {
                throw new IllegalArgumentException("Can't get AVG of empty column!");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        else{
            for (Double number : column){
                if(number != null) {
                    sum += number;
                }
            }
            return (sum / column.size());
        }
        return 0;
    }

    private double getDECIMALSumOfColumn(ArrayList<Double> column){
        double sum = 0;
        if(column.isEmpty()){
            try {
                throw new IllegalArgumentException("Can't get SUM of empty column!");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        else{
            for (double number : column){
                if((Double)number != null) {
                    sum += number;
                }
            }
            return sum;
        }
        return 0;
    }

    private int getINTSumOfColumn(ArrayList<Integer> column){
        int sum = 0;
        if(column.isEmpty()){
            try {
                throw new IllegalArgumentException("Can't get SUM of empty column!");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        else{
            for (Integer number : column){
                if(number != null) {
                    sum += number;
                }
            }
            return sum;
        }
        return 0;
    }


    private void toggleColumnReadLock(ArrayList<String> columnNames, boolean toggler){
        if (columnNames.size() > 0) {
            for (String columnName : columnNames) {
                if (toggler)
                    //in the project we only dealt with querying from one table
                    this.database.getColumnLocks(result.getFromTableNames()[0]).get(columnName).readLock().lock();
                else this.database.getColumnLocks(result.getFromTableNames()[0]).get(columnName).readLock().unlock();
            }
        } else throw new IllegalArgumentException("No column stated!");
    }




}
