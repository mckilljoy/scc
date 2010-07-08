package scc;

import java.lang.System;

// Cassandra imports
import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

// Thrift imports
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


// Java imports
import static org.apache.cassandra.thrift.ThriftGlue.*;
import java.io.UnsupportedEncodingException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

// Misc apache imports
import org.apache.commons.lang.ArrayUtils;

public class SimpleCassandraClient
{

    private TTransport transport = null;
    private Cassandra.Client thriftClient = null;

    private String server = null;
    private int port = 0;

    private String  clusterName = null;

    /*
    public SimpleCassandraClient(String server, int port)
    {
        this.server = server;
        this.port = port;

        this.connect();
    }
    */
    public boolean connect(String server, int port)
    {

        // Create the transport socket
        // We only have one type of socket
        transport = new TSocket(server, port);

        // Create he protocol and thrift client
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport, false, false);
        thriftClient = new Cassandra.Client(binaryProtocol);

        try
        {
            transport.open();
        }
        catch (Exception e)
        {

            System.out.println("Exception connecting to "+server+"/"+port+" - "+e.getMessage());

            return false;
        }

        // Lookup the cluster name, this is to make it clear which cluster the user is connected to
        try
        {
            clusterName = thriftClient.get_string_property("cluster name");
        }
        catch (Exception e)
        {

            System.out.println("Exception retrieving information about the cassandra node, check you have connected to the thrift port.");

            return false;
        }

        /*
        // save these and use them later
        // Extend the completer with keyspace and column family data.
        try
        {
            for (String keyspace : thriftClient_.get_string_list_property("keyspaces"))
            {
                // Ignore system column family
                if (keyspace.equals(SYSTEM_TABLE))
                    continue;

                for (String cf : cliClient_.getCFMetaData(keyspace).keySet())
                {
                    for (String cmd : completer_.getKeyspaceCommands())
                        completer_.addCandidateString(String.format("%s %s.%s", cmd, keyspace, cf));
                }
            }
        }
        catch (Exception e)
        {
            // Yes, we really do want to ignore any exceptions encountered here.
            if (css_.debug)
                e.printStackTrace();

            return;
        }
        */

        System.out.printf("Connected to: \"%s\" on %s/%d%n", clusterName, server, port);

        return true;
    }

    public void disconnect()
    {
        if (transport != null)
        {
            transport.close();
            transport = null;
        }
    }

    public boolean isConnected()
    {
        if (thriftClient == null)
        {
            return false;
        }
        return true;
    }

    // No supercolumn -- string version
    public byte[] get( String tableName,
                              String columnFamily,
                              String key,
                              String columnName )
    {
        return get( tableName, key, columnFamily, columnName.getBytes() );
    }

    // No supercolumn -- bytes version
    public byte[] get( String tableName,
                              String columnFamily,
                              String key,
                              byte[] columnName )
    {
        return get( tableName, key, columnFamily, null, columnName );
    }

    // Yes supercolumn -- string version
    public byte[] get( String tableName,
                              String columnFamily, 
                              String key,
                              String superColumnName,
                              String columnName )
    {
        return get( tableName, columnFamily, key, superColumnName.getBytes(), columnName.getBytes() );
    }

    // Yes supercolumn -- bytes version
    public byte[] get( String tableName,
                              String columnFamily, 
                              String key,
                              byte[] superColumnName,
                              byte[] columnName )
    {

        /*
        if (!(getCFMetaData(tableName).containsKey(columnFamily)))
        {
            css_.out.println("No such column family: " + columnFamily);
            return;
        }

        boolean isSuper = getCFMetaData(tableName).get(columnFamily).get("Type").equals("Super") ? true : false;
        */
        /*
        boolean isSuper = (superColumnName != null);
        
        // table.cf['key'] -- row slice
        if ( superColumnName == null &&
             columnName == null )
        {
            doSlice(tableName, key, columnFamily, superColumnName);
            return;
        }
        
        // table.cf['key']['column'] -- slice of a super, or get of a standard
        if ( superColumnName == null )
        {
            if (isSuper)
            {
                superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
                doSlice(tableName, key, columnFamily, superColumnName);
                return;
            }
            else
            {
                columnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            }
        }
        // table.cf['key']['column']['column'] -- get of a sub-column
        else 
        {
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            columnName = CliCompiler.getColumn(columnFamilySpec, 1).getBytes("UTF-8");
        }
        */
        try
        {
            //            System.out.println("getting " + superColumnName );
            // Perform a get(), print out the results.
            ColumnPath columnPath = createColumnPath(columnFamily, superColumnName, columnName);

            /*
            if( path.super_column == null )
            {
                
                System.out.println("using " + path.column_family + " " + 
                                   new String(path.column, "UTF-8") );

            }
            else
            {

                System.out.println("using " + path.column_family + " " + 
                                   new String(path.super_column, "UTF-8") + " " + 
                                   new String(path.column, "UTF-8") );

            }
            */

            Column column = thriftClient.get(tableName, key, columnPath, ConsistencyLevel.ONE).column;

            /*
            System.out.printf("=> (column=%s, value=%s, timestamp=%d)\n", 
                              //formatColumnName(tableName, columnFamily, column),
                              new String(column.name, "UTF-8"),
                              new String(column.value, "UTF-8"), 
                              column.timestamp);
            */

            return column.value;

        }
        catch (Exception e)
        {
            return null;
        }

    }

    // No super column
    public List<ColumnOrSuperColumn> getSlice(String keyspace, String columnFamily, String key)
    {
        return getSlice( keyspace, columnFamily, key, (byte[]) null);
    }

    // Yes super column -- string version
    public List<ColumnOrSuperColumn> getSlice(String keyspace, String columnFamily, String key, String superColumnName)
    {
        return getSlice( keyspace, columnFamily, key, superColumnName.getBytes() );
    }

    // Yes super column -- bytes version
    public List<ColumnOrSuperColumn> getSlice(String keyspace, String columnFamily, String key, byte[] superColumnName)
    //throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException, IllegalAccessException, NotFoundException, InstantiationException, ClassNotFoundException
    {

        try
        {

            SliceRange range = new SliceRange(new String().getBytes(), new String().getBytes(), true, 1000000);

            List<ColumnOrSuperColumn> columns = thriftClient.get_slice(keyspace, key, 
                                                                       createColumnParent(columnFamily, superColumnName),
                                                                       createSlicePredicate(null, range), ConsistencyLevel.ONE);
            /*
            int size = columns.size();
            System.out.println(size);

            // Print out super columns or columns.
            for (ColumnOrSuperColumn cosc : columns)
            {
                if (cosc.isSetSuper_column())
                {
                    SuperColumn superColumn = cosc.super_column;

                    System.out.printf("=> (super_column=%s,",
                                      //formatSuperColumnName(keyspace, columnFamily, superColumn));
                                      superColumn);

                    for (Column col : superColumn.getColumns())
                    {
                        System.out.printf("\n     (column=%s, value=%s, timestamp=%d)", 
                                          //formatSubcolumnName(keyspace, columnFamily, col),
                                          col,
                                          new String(col.value, "UTF-8"), 
                                          col.timestamp);
                    }

                    System.out.println(")"); 
                }
                else
                {
                    Column column = cosc.column;
                    System.out.printf("=> (column=%s, value=%s, timestamp=%d)\n", 
                                      //formatColumnName(keyspace, columnFamily, column),
                                      column,
                                      new String(column.value, "UTF-8"), 
                                      column.timestamp);
                }
            }
    
            System.out.println("Returned " + size + " results.");
            */
            return columns;

        }
        catch (Exception e)
        {
            return null;
        }

    }

    
    // No supercolumn -- string version
    // The 'value' data payload should still be in
    // bytes to avoid weird string converions issues
    public boolean insert( String tableName,
                                  String columnFamily,
                                  String key,
                                  String columnName,
                                  byte[] value )
    //        throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    {
        return insert( tableName, columnFamily, key, columnName.getBytes(), value );
    }

    // No supercolumn -- bytes version
    public boolean insert( String tableName,
                                  String columnFamily,
                                  String key,
                                  byte[] columnName,
                                  byte[] value )
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    {
        return insert( tableName, columnFamily, key, null, columnName, value );
    }

    // Yes supercolumn -- string version
    public boolean insert( String tableName,
                                  String columnFamily,
                                  String key,
                                  String superColumnName,
                                  String columnName,
                                  byte[] value )
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    {
        return insert( tableName, columnFamily, key, superColumnName.getBytes(), columnName.getBytes(), value );
    }

    // Yes supercolumn -- bytes version
    public boolean insert( String tableName,
                                  String columnFamily,
                                  String key,
                                  byte[] superColumnName,
                                  byte[] columnName,
                                  byte[] value )
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    {
        try
        {
            /*
            System.out.println("Inserting " + 
                               tableName + " " +
                               columnFamily + " " +
                               key + " " +
                               superColumnName + " " +
                               columnName + " " +
                               value );
            */

            ColumnPath columnPath = createColumnPath(columnFamily, superColumnName, columnName);
            //System.out.println( "insert " + columnPath );            
            thriftClient.insert(tableName, key, columnPath, value, timestampMicros(), ConsistencyLevel.ONE);
            //System.out.println( "insert " + columnPath );            

        }
        catch (Exception e)
        {
            //System.out.println("Insert exception");
            return false;
        }
        //        catch (InvalidRequestException e)
        //        {
        //            return false;
        //        }

        return true;

    }

    public List<String> list()
    {

        try
        {
            List<String> tables = thriftClient.get_string_list_property("keyspaces");

            System.out.println("Keyspaces in this cluster:");

            for (String table : tables)
            {
                System.out.println(table);
            }

            return tables;

        }
        catch (Exception e)
        {
            return null;
        }

    }

    public Map<String, Map<String, String>> describe( String tableName ) 
    //        throws TException
    {

        try
        {

            System.out.println("Describing keyspace: " + tableName);

            Map<String, Map<String, String>> columnFamiliesMap;

            columnFamiliesMap = thriftClient.describe_keyspace(tableName);

            for (String columnFamilyName: columnFamiliesMap.keySet()) {
                Map<String, String> columnMap = columnFamiliesMap.get(columnFamilyName);
                String desc = columnMap.get("Desc");
                String columnFamilyType = columnMap.get("Type");
                String sort = columnMap.get("CompareWith");
                String flushperiod = columnMap.get("FlushPeriodInMinutes");
                System.out.println(desc);
                System.out.println("Column Family Type: " + columnFamilyType);
                System.out.println("Column Sorted By: " + sort);
                System.out.println("flush period: " + flushperiod + " minutes");
                System.out.println("------");
            }

            return columnFamiliesMap;

        }
        catch (Exception e)
        {
            return null;
        }

    }

    // No supercolumn -- string version
    public boolean delete(String tableName, String columnFamily, String key, String columnName)
    {
        return delete( tableName, columnFamily, key, null, columnName.getBytes() );
    }

    // No supercolumn -- bytes version
    public boolean delete(String tableName, String columnFamily, String key, byte[] columnName)
    {
        return delete( tableName, columnFamily, key, (byte[])null, columnName );
    }

    // Yes supercolumn -- string version
    public boolean delete(String tableName, String columnFamily, String key, String superColumnName, String columnName)
    {
        return delete( tableName, columnFamily, key, superColumnName.getBytes(), columnName.getBytes() );
    }

    // Yes supercolumn -- bytes version
    public boolean delete(String tableName, String columnFamily, String key, byte[] superColumnName, byte[] columnName)
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    {


        /*
        try
        {
            if (!(getCFMetaData(tableName).containsKey(columnFamily)))
            {
                css_.out.println("No such column family: " + columnFamily);
                return;
            }
            
            isSuper = getCFMetaData(tableName).get(columnFamily).get("Type").equals("Super") ? true : false;
        }
        catch (NotFoundException nfe)
        {
            css_.out.printf("No such keyspace: %s\n", tableName);
            return;
        }
     
        if ((columnSpecCnt < 0) || (columnSpecCnt > 2))
        {
            css_.out.println("Invalid row, super column, or column specification.");
            return;
        }
        
        if (columnSpecCnt == 1)
        {
            // table.cf['key']['column']
            if (isSuper)
                superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            else
                columnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
        }
        else if (columnSpecCnt == 2)
        {
            // table.cf['key']['column']['column']
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            columnName = CliCompiler.getColumn(columnFamilySpec, 1).getBytes("UTF-8");
        }
        */

        try 
        {

            ColumnPath columnPath = createColumnPath(columnFamily, superColumnName, columnName);
            
            thriftClient.remove(tableName, key, columnPath, timestampMicros(), ConsistencyLevel.ONE);
            System.out.println("Removed");
         
            return true;
   
        }
        catch (Exception e)
        {
            return false;
        }

    }


    //
    // Misc helper functions
    //
    public static long timestampMicros()
    {
        // we use microsecond resolution for compatibility with other client libraries, even though
        // we can't actually get microsecond precision.
        return System.currentTimeMillis() * 1000;
    }

    public static long timestampMS()
    {
        // we use microsecond resolution for compatibility with other client libraries, even though
        // we can't actually get microsecond precision.
        return System.currentTimeMillis() * 1000;
    }



    /*
    private void doSlice(String keyspace, String key, String columnFamily, byte[] superColumnName)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException, IllegalAccessException, NotFoundException, InstantiationException, ClassNotFoundException
    {
        SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 1000000);
        List<ColumnOrSuperColumn> columns = thriftClient.get_slice(keyspace, key, 
            createColumnParent(columnFamily, superColumnName),
            createSlicePredicate(null, range), ConsistencyLevel.ONE);
        int size = columns.size();
        
        // Print out super columns or columns.
        for (ColumnOrSuperColumn cosc : columns)
        {
            if (cosc.isSetSuper_column())
            {
                SuperColumn superColumn = cosc.super_column;

                System.out.printf("=> (super_column=%s,",
                                  //formatSuperColumnName(keyspace, columnFamily, superColumn));
                                  superColumn);

                for (Column col : superColumn.getColumns())
                    System.out.printf("\n     (column=%s, value=%s, timestamp=%d)", 
                                      //formatSubcolumnName(keyspace, columnFamily, col),
                                      col,
                                      new String(col.value, "UTF-8"), 
                                      col.timestamp);
                
                System.out.println(")"); 
            }
            else
            {
                Column column = cosc.column;
                System.out.printf("=> (column=%s, value=%s, timestamp=%d)\n", 
                                  //formatColumnName(keyspace, columnFamily, column),
                                  column,
                                  new String(column.value, "UTF-8"), 
                                  column.timestamp);
            }
        }
        
        System.out.println("Returned " + size + " results.");
    }

    private String formatSuperColumnName(String keyspace, String columnFamily, SuperColumn column) throws NotFoundException, TException, ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        return getFormatTypeForColumn(getCFMetaData(keyspace).get(columnFamily).get("CompareWith")).getString(column.name);
    }

    private String formatSubcolumnName(String keyspace, String columnFamily, Column subcolumn) throws NotFoundException, TException, ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        return getFormatTypeForColumn(getCFMetaData(keyspace).get(columnFamily).get("CompareSubcolumnsWith")).getString(subcolumn.name);
    }

    private String formatColumnName(String keyspace, String columnFamily, Column column) throws ClassNotFoundException, NotFoundException, TException, IllegalAccessException, InstantiationException
    {
        return getFormatTypeForColumn(getCFMetaData(keyspace).get(columnFamily).get("CompareWith")).getString(column.name);
    }

    private AbstractType getFormatTypeForColumn(String compareWith) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        AbstractType type;

        try 
        {
            type = (AbstractType) Class.forName(compareWith).newInstance();
        }
        catch (ClassNotFoundException e)
        {
            type = BytesType.class.newInstance();
        }

        return type;
    }
    */
}