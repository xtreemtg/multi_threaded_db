package datastructures.projects.database;
/*
 *This class that takes the parsed query and creates a btree for that column. before executing the logic, we
 * lock the column, since we update the BTree with that column's contents, and we dont want the column changing.
 */


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.CreateIndexQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;
import net.sf.jsqlparser.JSQLParserException;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryCreateIndex {

    private CreateIndexQuery result;
    private Database database;
    private ArrayListTable table;
    QueryCreateTable tableInfo;

    public QueryCreateIndex(CreateIndexQuery result)throws JSQLParserException {
        this.result = result;
        this.database = DBDriver.database;
        this.table = database.getTable(result.getTableName());
        if(this.table == null) return;
        this.tableInfo = (QueryCreateTable) database.getInfoMap().get(result.getTableName()).get("tableDescription");

        HashMap<String, BTree> mainMap = database.getBtreeMap().get(result.getTableName());
        if (mainMap == null || !mainMap.containsKey(result.getColumnName())) {
            //setIndex();
        }
    }

    public QueryCreateTable getTableInfo() {
        return tableInfo;
    }

    public ArrayListTable getTable() {
        return table;
    }


    public boolean setIndex(){
        try {
            this.database.getColumnLocks(result.getTableName()).get(result.getColumnName()).writeLock().lock(); //lock the column

            if (tableInfo.getIntMap().containsKey(result.getColumnName())) {
                BTree<Integer, ArrayList<ArrayList<Integer>>> btree = new BTree<Integer, ArrayList<ArrayList<Integer>>>();
                int columnIndex = table.getColumnIndex(result.getColumnName());

                for (int i = 0; i < table.getNumberOfRows(); i++) {
                    ArrayList currentRow = table.getRow(i);
                    Integer key = (Integer) currentRow.get(columnIndex);
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
                database.storeBTree(result.getTableName(), result.getColumnName(), btree);
                database.getBtreeLocks(result.getTableName()).put(result.getColumnName(), new ReentrantReadWriteLock(true));
                return true;
            } else if (tableInfo.getDoubleMap().containsKey(result.getColumnName())) {
                BTree<Double, ArrayList<ArrayList<Double>>> btree = new BTree<Double, ArrayList<ArrayList<Double>>>();
                int columnIndex = table.getColumnIndex(result.getColumnName());

                for (int i = 0; i < table.getNumberOfRows(); i++) {
                    ArrayList currentRow = table.getRow(i);
                    Double key = (Double) currentRow.get(columnIndex);
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
                database.storeBTree(result.getTableName(), result.getColumnName(), btree);
                database.getBtreeLocks(result.getTableName()).put(result.getColumnName(), new ReentrantReadWriteLock(true));
                return true;
            } else if (tableInfo.getBooleanMap().containsKey(result.getColumnName())) {
                BTree<Boolean, ArrayList<ArrayList<Boolean>>> btree = new BTree<Boolean, ArrayList<ArrayList<Boolean>>>();
                int columnIndex = table.getColumnIndex(result.getColumnName());

                for (int i = 0; i < table.getNumberOfRows(); i++) {
                    ArrayList currentRow = table.getRow(i);
                    Boolean key = (Boolean) currentRow.get(columnIndex);
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
                database.storeBTree(result.getTableName(), result.getColumnName(), btree);
                database.getBtreeLocks(result.getTableName()).put(result.getColumnName(), new ReentrantReadWriteLock(true));
                return true;
            } else {
                BTree<String, ArrayList<ArrayList<String>>> btree = new BTree<String, ArrayList<ArrayList<String>>>();
                if (table.getColNameMap().containsKey(result.getColumnName())) {
                    int columnIndex = table.getColumnIndex(result.getColumnName());

                    for (int i = 0; i < table.getNumberOfRows(); i++) {
                        ArrayList currentRow = table.getRow(i);
                        String key = (String) currentRow.get(columnIndex);
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
                    database.storeBTree(result.getTableName(), result.getColumnName(), btree);
                }
                return false;
            }
        } finally {
            this.database.getColumnLocks(result.getTableName()).get(result.getColumnName()).writeLock().unlock();
        }


    }


}
