package edu.yu.cs.dataStructures.fall2016.SimpleSQLParser;

import java.io.Serializable;

/**
 * @author diament@yu.edu
 *
 */
public abstract class SQLQuery implements Serializable
{
    private String queryString;

    SQLQuery(String query)
    {
	this.queryString = query;
    }
    
    public String getQueryString()
    {
	return this.queryString;
    }
    public String toString()
    {
	return this.getQueryString();
    }
    public abstract ColumnValuePair[] getColumnValuePairs();
    abstract void addColumnValuePair(ColumnID col, String value);
}