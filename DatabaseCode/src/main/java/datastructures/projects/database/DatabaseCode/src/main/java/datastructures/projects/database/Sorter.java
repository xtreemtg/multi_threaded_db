package datastructures.projects.database;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SelectQuery;

import java.util.ArrayList;
import java.util.HashMap;


//**** got main code from
// http://www.codexpedia.com/java/java-merge-sort-implementation/******

public class Sorter {

    /*Function to sort array using insertion sort*/
    static ArrayList<HashMap<Comparable, Integer>> insertionSort(ArrayListTable table, ArrayList<HashMap<Comparable, Integer>> list, SelectQuery.OrderBy[] orderBys, int index, boolean is, boolean isnt) {
        int n = list.size();
        for (int i = 1; i < n; i++) {
            if(list.get(i) == null) continue;
            HashMap key = list.get(i);
            int j = i - 1;

            if (is) {
                while (j >= 0 && isLess((Comparable) (key.keySet().toArray()[0]), (Comparable) list.get(j).keySet().toArray()[0])) {
                    boolean canSwap = false;
                    if(index > 0)
                        canSwap = iterateEachColumn(table, orderBys, index, j, key, list);

                    if(!canSwap) {
                        j = j - 1;
                        continue;
                    }
                    list.set(j + 1, list.get(j));
                    j = j - 1;
                    list.set(j + 1, key);
                }


            } else if (isnt) {
                while (j >= 0 && isLess((Comparable) list.get(j).keySet().toArray()[0], (Comparable) key.keySet().toArray()[0])) {
                    boolean canSwap = true;
                    if(index > 0)
                        canSwap = iterateEachColumn(table, orderBys, index, j, key, list);

                    if(!canSwap) {
                        j = j - 1;
                        continue;
                    }
                    list.set(j + 1, list.get(j));
                    j = j - 1;
                    list.set(j + 1, key);
                }
            }

        }
        return list;
    }

    private static boolean isLess(Comparable input, Comparable columnValue) {
        if (input == null || columnValue == null) return false;
        return input.compareTo(columnValue) < 0;
    }
    private static boolean isEqual(Comparable input, Comparable columnValue) {
        if (input == null || columnValue == null) return false;
        return input.compareTo(columnValue) == 0;
    }

    private static boolean iterateEachColumn(ArrayListTable table, SelectQuery.OrderBy[] orderBys, int index, int j, HashMap key, ArrayList<HashMap<Comparable, Integer>> list){
        for(int k = index - 1; k >= 0; k--){
            String name = orderBys[k].getColumnID().getColumnName();
            ArrayList column = table.getColumnByName(name);
            Object x = column.get((Integer) key.values().toArray()[0]);
            Object y = column.get((Integer)list.get(j).values().toArray()[0]);
            if(isEqual((Comparable)x, (Comparable)y)){
                if (k !=0)
                    return iterateEachColumn(table, orderBys, k, j, key, list);
                else return true;
            } else{
                return false;
            }
        }
        return true;
    }


}