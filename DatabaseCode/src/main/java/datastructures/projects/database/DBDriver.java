package datastructures.projects.database;

/*
 *This class parses the query and does a ton of logic on it and returns a ResultSet back.
 * It also stores information in the database and saves that database to a file on drive.
 */


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.*;
import net.sf.jsqlparser.JSQLParserException;

import java.io.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;




public class DBDriver  {

    private static SQLParser parser = new SQLParser();

    static Database database;

    public static ResultSet execute(String query) throws JSQLParserException{
        SQLQuery targum;
        try {
           targum = parser.parse(query);
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
        if (targum.getQueryString().startsWith("CREATE TABLE")){
            try {
                QueryCreateTable ctq = new QueryCreateTable((CreateTableQuery) targum);
                ResultSet resultSet = new ResultSet(query);
                boolean result;
                result = database.storeTable(ctq, ctq.getTableName());
                resultSet.setColumns((ctq.getResultSetColNames()));
                resultSet.getColumnTypes().add(ctq.getResultSetColTypes());
                resultSet.setTable(ctq.getTable().getTable());
                resultSet.setQueryResult(result);
                resultSet.makeStringResult();
                System.out.println("Columns in " + ctq.getTableName() + ":" + resultSet.getColumns());
                System.out.println("Column types in " + ctq.getTableName() + ":" + resultSet.getColumnTypes());
                System.out.println();
                saveDatabase();
                return resultSet;
            } catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }
        else if(targum.getQueryString().startsWith("CREATE INDEX")){
            CreateIndexQuery q = (CreateIndexQuery)targum;
            ReentrantReadWriteLock l = database.getTableLock().get(q.getTableName()); //acquire and lock the table lock
            try {
                l.readLock().lock();
                QueryCreateIndex createindexquery = new QueryCreateIndex(q);
                ResultSet resultSet = new ResultSet(query);
                boolean result = createindexquery.setIndex();
                resultSet.setColumns((createindexquery.getTableInfo().getResultSetColNames()));
                resultSet.setTable(createindexquery.getTable().getTable());
                resultSet.setQueryResult(result);
                resultSet.makeStringResult();
                System.out.println("CREATE INDEX result: " + result);
                System.out.println();
                saveDatabase();
                return resultSet;
            } catch (Exception e){
                e.printStackTrace();
                return null;
            } finally {
                l.readLock().unlock();
            }


        }
        else if(targum.getQueryString().startsWith("INSERT")){
            ReentrantReadWriteLock l = null;
            try {
                InsertQuery q = (InsertQuery)targum;
                l = database.getTableLock().get(q.getTableName()); //acquire and lock the table lock
                l.readLock().lock();
                QueryInsert insertquery = new QueryInsert(q);
                ResultSet resultSet = new ResultSet(query);
                boolean result;
                if (insertquery.table() == null) result = false; //there needs to be a table to insert stuff into!
                else {
                    result = insertquery.insertRows();
                    resultSet.setColumns(insertquery.getCtq().getResultSetColNames());
                }
                resultSet.setTable(insertquery.table().getTable());
                resultSet.setQueryResult(result);
                resultSet.makeStringResult();
                System.out.println("INSERT result: " + result);
                System.out.println();
                saveDatabase();
                return resultSet;
            } catch (Exception e){
                e.printStackTrace();
                return null;
            } finally {
                if (l != null) {
                    l.readLock().unlock();
                }
            }

        }
        else if(targum.getQueryString().startsWith("UPDATE")){
            UpdateQuery q = (UpdateQuery)targum;
            ReentrantReadWriteLock l = null;
            try {
                l  = database.getTableLock().get(q.getTableName());
                l.readLock().lock(); //acquire and lock the table lock
                QueryUpdate updatequery = new QueryUpdate(q);
                ResultSet resultSet;
                boolean result = updatequery.update();
                resultSet = updatequery.getResultSet();
                resultSet.setQueryResult(result);
                resultSet.makeStringResult();
                System.out.println("UPDATE result: " + result);
                System.out.println();
                saveDatabase();
                return resultSet;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }finally
             {
                 if(l != null) {
                     l.readLock().unlock();
                 }
            }

        }
        else if(targum.getQueryString().startsWith("DELETE")){
            DeleteQuery q = (DeleteQuery)targum;
            ReentrantReadWriteLock l = database.getTableLock().get(q.getTableName());
            try {
                l.writeLock().lock();
                QueryDelete deletequery = new QueryDelete(q);
                ResultSet resultSet;
                boolean result = deletequery.delete();
                //resultSet.getColumns().add(deletequery.getCtq().getResultSetColNames());
                //resultSet.getTable().get(0).add(result);
                resultSet = deletequery.getResultSet();
                resultSet.setQueryResult(result);
                resultSet.makeStringResult();
                System.out.println("DELETE result: " + result);
                System.out.println();
                saveDatabase();
                return resultSet;
            } catch (Exception e){
                e.printStackTrace();
                return null;
            }finally {
                l.writeLock().unlock();
            }

        }
        return null;
    }

    public static ResultSet executeSELECT(String query) throws JSQLParserException {
        try {
            SQLQuery targum = parser.parse(query);
            if (targum.getQueryString().startsWith("SELECT")) {
                SelectQuery q = (SelectQuery) targum;
                ResultSet resultSet = new ResultSet(query);
                if (!database.getTableLock().containsKey(q.getFromTableNames()[0])) {
                    System.out.print("Table " + q.getFromTableNames()[0] + " doesn't exist!");
                    resultSet.setQueryResult(false);
                    return resultSet;
                }
                ReentrantReadWriteLock tableLock = database.getTableLock().get(q.getFromTableNames()[0]);
                //my project only dealt with 1 table
                // so hence we lock the table we are dealing with
                if (tableLock.isWriteLocked()) System.out.println("waiting for edits to finish..");
                try {
                    tableLock.readLock().lock();
                    QuerySelect selectquery = new QuerySelect(q);
                    resultSet = selectquery.getResultSet();
                    resultSet.makeStringResult();
                    System.out.println("SELECT result: " + resultSet.getQueryResult());
                    System.out.println();
                    return resultSet;
                } finally {
                    tableLock.readLock().unlock();
                }

            } else System.out.println("No valid query recognized!");
            return null;
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /* method that saves the database to the drive in
    * a file as bunch of bytes. Everytime the server restarts it'll take the latest copy
    * of the database. (it should be the most up to date since we save
    * everytime we make a write to the database.
     */
    private static void saveDatabase(){
        try {
            FileOutputStream f = new FileOutputStream("Database.txt");
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(database);
            o.close();
            f.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getSavedDatabase(){
        try {
            FileInputStream f = new FileInputStream("Database.txt");
            ObjectInputStream in = new ObjectInputStream(f);
            database = (Database) in.readObject();
            in.close();
            f.close();
        } catch (Exception e){
            System.out.println("No Database detected. Creating a new one...");
            database = new Database(); //if file doesnt exist then we make a fresh instance of database
            saveDatabase();
        }
    }


}

