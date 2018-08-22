package datastructures.projects.database;

/*
ResultSet is the object returned after all the database logic is executed.
        This object has a list of lists which is the table, as well as
        a list of the column types, and a list of the actual columns
 */

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;
import net.sf.jsqlparser.JSQLParserException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class ResultSet {

    private ArrayList columns;
    private ArrayList columnTypes;
    private ArrayList<ArrayList> table;
    private Object queryResult;



    public ResultSet(){
        columns = new ArrayList();
        columnTypes = new ArrayList();
        table = new ArrayList<ArrayList>();
        ArrayList row = new ArrayList();
        table.add(row);

    }

    public void setQueryResult(Object queryResult) {
        this.queryResult = queryResult;
    }

    public Object queryResult(){
        return this.queryResult;
    }

    public ArrayList getColumns() {
        return columns;
    }

    public ArrayList getColumnTypes() {
        return columnTypes;
    }

    public ArrayList<ArrayList> getTable() {
        return table;
    }

    public void setTable(ArrayList<ArrayList> table) {
        this.table = table;
    }

    public void setColumns(ArrayList<String> columns){
        this.columns = columns;
    }

    public void printWholeTable(){
        System.out.println("----------------------------------------------------------");
        for(int r = 0; r < table.size(); r++){
            for(int c = 0; c < table.get(r).size(); c++){
                Object element = table.get(r).get(c);
                if(element !=  null) {
                    System.out.print(table.get(r).get(c) + " | ");
                }
                else{
                    System.out.print( " | ");
                }

            }
            System.out.println();
        }
        System.out.println("-----------------------------------------------------------");

    }

    public void printWholeTable2() {
        final Object[][] table = new Object[this.table.size() + 2][];
        table[0] = this.columns.toArray();
        table[1] = new ArrayList<String>(Collections.nCopies(columns.size(), "----")).toArray();
        for(int i = 0; i < this.table.size(); i++) {
            ArrayList row = this.table.get(i);
            table[i + 2] = row.toArray();
        }

        StringBuilder topBottom = new StringBuilder();
        StringBuilder format = new StringBuilder();
        for(int i = 0; i < columns.size(); i++){
            format.append("%-15s");
            topBottom.append("--------------");
        }
        format.append("\n");

        System.out.println(topBottom);
        for (final Object[] row : table) {
            System.out.format(format.toString(), row);
        }
        System.out.println(topBottom);
    }
}
