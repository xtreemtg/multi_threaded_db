
package datastructures.projects.database;
/*
 *This class simply stores all the tables and BTrees
 */

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.CreateTableQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database implements Serializable{
    private ArrayList<ArrayListTable> tableStorage;
    private HashMap<String, HashMap<String, Object>> infoMap;
    private HashMap<String, HashMap<String, BTree>> mainBTreeMap;
    private HashMap<String, BTree> btreeMap;
    private HashMap<String, ReentrantReadWriteLock> lockForTables;

    public Database(){
        tableStorage = new ArrayList<>();
        infoMap = new HashMap<>();
        mainBTreeMap = new HashMap<>();
        btreeMap = new HashMap<>();
        lockForTables = new HashMap<>();

    }

    public boolean storeTable(QueryCreateTable ctq, String tablename){

        if(!infoMap.containsKey(tablename)) {
            tableStorage.add(ctq.getTable());
            HashMap<String, Object> tableInfoMap = new HashMap<>();
            HashMap<String, ReentrantReadWriteLock> lockList = new HashMap<>();
            for (String columnName : ctq.getResultSetColNames()){
                lockList.put(columnName, new ReentrantReadWriteLock(true));
            }
            tableInfoMap.put("tableDescription", ctq);
            tableInfoMap.put("columnLocks", lockList);
            tableInfoMap.put("rowLocks", new HashMap<Integer, ReentrantReadWriteLock>());
            infoMap.put(tablename, tableInfoMap);
            return true;
        }
        System.out.println("Table already exists!");
        return false;
    }

    public HashMap<String, HashMap<String, BTree>> getBtreeMap(){
        return mainBTreeMap;
    }

    public void storeBTree(String tableName, String columnName, BTree bTree){
        HashMap<String, BTree> map= new HashMap<String, BTree>();
        btreeMap.put(columnName, bTree);
        mainBTreeMap.put(tableName, btreeMap);
    }

    public ArrayListTable getTable(String tableName){
        for(ArrayListTable table : tableStorage)
        {
            if (table.getTableName().equals(tableName)){
                return table;
            }

        }
        //throw new NullPointerException("No such table exists!");
        return null;
    }

    public ArrayList<ArrayListTable> getTableStorage(){
        return tableStorage;
    }

    public HashMap<String, HashMap<String, Object>> getInfoMap() {
        return infoMap;
    }

    public void setTableLock(String tableName, ReentrantReadWriteLock lock) {
        this.lockForTables.put(tableName, lock);
    }

    public HashMap<String, ReentrantReadWriteLock> getTableLock() {
        return lockForTables;
    }

    public HashMap<String, ReentrantReadWriteLock> getColumnLocks(String tableName) {
        return (HashMap<String, ReentrantReadWriteLock>) this.infoMap.get(tableName).get("columnLocks");
    }

    public HashMap<Integer, ReentrantReadWriteLock> getRowLocks(String tableName) {
        return (HashMap<Integer, ReentrantReadWriteLock>) this.infoMap.get(tableName).get("rowLocks");
    }

}
