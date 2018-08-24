package datastructures.projects.database;

/*
 *This class that takes the parsed query and creates a table with all it's information, and stores that
 * information in the database. New to this project is the addition of read/write locks
 */

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.CreateTableQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;
import net.sf.jsqlparser.JSQLParserException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import net.sf.jsqlparser.schema.Column;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryCreateTable implements Serializable {
    private CreateTableQuery result;
    private ColumnDescription[] description;
    private ArrayListTable table;
    private String tableName;
    private String pColumnName;
    private ArrayList<String> resultSetColNames = new ArrayList<String>();
    private ArrayList<String> resultSetColTypes = new ArrayList<String>();
    private HashMap<String, Boolean> doubleMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> intMap = new HashMap<String, Boolean>();
    private HashMap<String, Boolean> booleanMap = new HashMap<String, Boolean>();
    private Database database;


    public QueryCreateTable(CreateTableQuery result) throws JSQLParserException {

        this.result = result;
        this.tableName = result.getTableName();
        this.table = new ArrayListTable(this.tableName);
        this.description = result.getColumnDescriptions();
        this.pColumnName = result.getPrimaryKeyColumn().getColumnName();
        this.database = DBDriver.database;
        setColumns();
        setColumnTypes();
        setPrimaryKey();
        setIndex();
        setLocks();

    }



    public void setPrimaryKey(){
        table.setPrimaryColumnName(pColumnName);

    }
    // each table gets a read/write lock and will lock whenever the table is accessed
    public void setLocks(){
        database.setTableLock(this.tableName, new ReentrantReadWriteLock(true));
    }

    public void setColumnTypes(){
        for (ColumnDescription description : description) {
            if (description.getColumnType().toString().equals("DECIMAL")) {
                doubleMap.put(description.getColumnName(), true);
            }
            else if (description.getColumnType().toString().equals("INT")){
                intMap.put(description.getColumnName(), true);
            }
            else if(description.getColumnType().toString().equals("BOOLEAN")){
                booleanMap.put(description.getColumnName(), true);
            }
        }
    }

    public HashMap<String, Boolean> getBooleanMap() {
        return booleanMap;
    }

    public HashMap<String, Boolean> getDoubleMap() {
        return doubleMap;
    }

    public HashMap<String, Boolean> getIntMap() {
        return intMap;
    }

    public ArrayListTable getTable(){
        return table;
    }

    public ColumnDescription[] getTableInfo(){
        return result.getColumnDescriptions();
    }

    public String getTableName() {
        return tableName;
    }

    public ArrayList<String> getResultSetColNames() {
        return resultSetColNames;
    }

    public ArrayList<String> getResultSetColTypes() {
        return resultSetColTypes;
    }

    public void setColumns(){
        for(ColumnDescription descript : description){
            table.addColumnNames(descript.getColumnName());
            resultSetColNames.add(descript.getColumnName());
            resultSetColTypes.add(descript.getColumnType().toString());
        }
        table.setColumnNames(resultSetColNames);


    }



    public ColumnDescription[] getDescription() {
        return description;
    }

    public CreateTableQuery getResult() {
        return result;
    }

    public void setIndex(){
        if (intMap.containsKey(pColumnName)){
            BTree<Integer, ArrayList<ArrayList<Integer>>> btree = new BTree<Integer, ArrayList<ArrayList<Integer>>>();
            int columnIndex = table.getColumnIndex(pColumnName);

            for(int i = 0; i < table.getNumberOfRows(); i++){
                ArrayList currentRow = table.getRow(i);
                Integer key = (Integer)currentRow.get(columnIndex);
                if (key != null) {
                    ArrayList<ArrayList<Integer>> listOfValues = btree.get(key);

                    if (listOfValues != null) {
                        listOfValues.add(currentRow);
                    } else {
                        ArrayList<ArrayList<Integer>> newlistOfValues = new ArrayList<ArrayList<Integer>>();
                        newlistOfValues.add(currentRow);
                        btree.put(key, newlistOfValues);
                    }
                }
            }
            database.storeBTree(result.getTableName(), pColumnName, btree);
        }
        else if (doubleMap.containsKey(pColumnName)){
            BTree<Double, ArrayList<ArrayList<Double>>> btree = new BTree<Double, ArrayList<ArrayList<Double>>>();
            int columnIndex = table.getColumnIndex(pColumnName);

            for(int i = 0; i < table.getNumberOfRows(); i++){
                ArrayList currentRow = table.getRow(i);
                Double key = (Double)currentRow.get(columnIndex);
                if (key != null) {
                    ArrayList<ArrayList<Double>> listOfValues = btree.get(key);

                    if (listOfValues != null) {
                        listOfValues.add(currentRow);
                    } else {
                        ArrayList<ArrayList<Double>> newlistOfValues = new ArrayList<ArrayList<Double>>();
                        newlistOfValues.add(currentRow);
                        btree.put(key, newlistOfValues);
                    }
                }
            }
            database.storeBTree(result.getTableName(), pColumnName, btree);
        }
        else if (booleanMap.containsKey(pColumnName)){
            BTree<Boolean, ArrayList<ArrayList<Boolean>>> btree = new BTree<Boolean, ArrayList<ArrayList<Boolean>>>();
            int columnIndex = table.getColumnIndex(pColumnName);

            for(int i = 0; i < table.getNumberOfRows(); i++){
                ArrayList currentRow = table.getRow(i);
                Boolean key = (Boolean)currentRow.get(columnIndex);
                if (key != null) {
                    ArrayList<ArrayList<Boolean>> listOfValues = btree.get(key);

                    if (listOfValues != null) {
                        listOfValues.add(currentRow);
                    } else {
                        ArrayList<ArrayList<Boolean>> newlistOfValues = new ArrayList<ArrayList<Boolean>>();
                        newlistOfValues.add(currentRow);
                        btree.put(key, newlistOfValues);
                    }
                }



            }
            database.storeBTree(result.getTableName(), pColumnName, btree);
        }
        else {
            BTree<String, ArrayList<ArrayList<String>>> btree = new BTree<String, ArrayList<ArrayList<String>>>();
            int columnIndex = table.getColumnIndex(pColumnName);

            for(int i = 0; i < table.getNumberOfRows(); i++){
                ArrayList currentRow = table.getRow(i);
                String key = (String)currentRow.get(columnIndex);
                if (key != null) {
                    ArrayList<ArrayList<String>> listOfValues = btree.get(key);

                    if (listOfValues != null) {
                        listOfValues.add(currentRow);
                    } else {
                        ArrayList<ArrayList<String>> newlistOfValues = new ArrayList<ArrayList<String>>();
                        newlistOfValues.add(currentRow);
                        btree.put(key, newlistOfValues);
                    }
                }

            }
            database.storeBTree(result.getTableName(), pColumnName, btree);
        }


    }
}
