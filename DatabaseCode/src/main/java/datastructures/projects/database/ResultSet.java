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

public class ResultSet {

    private ArrayList columns;
    private ArrayList columnTypes;
    private ArrayList<ArrayList> table;



    public ResultSet(){
        columns = new ArrayList();
        columnTypes = new ArrayList();
        table = new ArrayList<ArrayList>();
        ArrayList row = new ArrayList();
        table.add(row);

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
}
