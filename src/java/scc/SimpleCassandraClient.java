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

    public String server = null;
    public int port = 0;

    public String clusterName = null;

    /*
    public SimpleCassandraClient(String server, int port)
    {
        this.server = server;
        this.port = port;

        this.connect();
    }
    */
    public void connect(String server, int port)
        throws CassandraException
    {

        this.server = server;
        this.port = port;

        TBinaryProtocol binaryProtocol;
        

        try
        {
            //
            // Create the transport socket
            // We only have one type of socket
            //
            transport = new TSocket( server, port );

            //
            // Create the protocol and thrift client
            //
            binaryProtocol = new TBinaryProtocol( transport, false, false );

            //
            // Create a thrift client with the protocol
            //
            thriftClient = new Cassandra.Client( binaryProtocol );

            transport.open();
        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception opening transport on " +
                                          server + "/" +
                                          port + ": " +
                                          e.getMessage() );

        }

        // Lookup the cluster name, this is to make it clear which cluster the user is connected to
        try
        {
            clusterName = thriftClient.get_string_property("cluster name");
        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception getting cluster name on " +
                                          server + "/" +
                                          port  + ": " +
                                          e.getMessage() );

        }

    }

    public void disconnect()
    {
        if (transport != null)
        {
            transport.close();
            transport = null;
            thriftClient = null;
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
    public byte[] get( String keyspace,
                       String columnFamily,
                       String key,
                       String columnName )
        throws CassandraException
    {
        return get( keyspace, key, columnFamily, columnName.getBytes() );
    }

    // No supercolumn -- bytes version
    public byte[] get( String keyspace,
                       String columnFamily,
                       String key,
                       byte[] columnName )
        throws CassandraException
    {
        return get( keyspace, key, columnFamily, null, columnName );
    }

    // Yes supercolumn -- string version
    public byte[] get( String keyspace,
                       String columnFamily, 
                       String key,
                       String superColumnName,
                       String columnName )
        throws CassandraException
    {

        if( superColumnName == null )
        {
            return get( keyspace, columnFamily, key, null, columnName.getBytes() );
        }
        else
        {
            return get( keyspace, columnFamily, key, superColumnName.getBytes(), columnName.getBytes() );
        }
    }

    // Yes supercolumn -- bytes version
    public byte[] get( String keyspace,
                       String columnFamily, 
                       String key,
                       byte[] superColumnName,
                       byte[] columnName )
        throws CassandraException
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
            //
            // Create the column path and execute the get
            //
            ColumnPath columnPath = createColumnPath(columnFamily, superColumnName, columnName);

            Column column = thriftClient.get(keyspace, key, columnPath, ConsistencyLevel.ONE).column;

            return column.value;

        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception getting " +
                                          keyspace + "|" +
                                          columnFamily + "|" +
                                          key + "|" +
                                          superColumnName + "|" +
                                          columnName + ": " +
                                          e.getMessage() );

        }

    }

    // No super column
    //public List<ColumnOrSuperColumn> getSlice(String keyspace, String columnFamily, String key)
    public HashMap getSlice( String keyspace,
                             String columnFamily,
                             String key )
        throws CassandraException
    {
        return getSlice( keyspace, columnFamily, key, (byte[]) null);
    }

    // Yes super column -- string version
    public HashMap getSlice( String keyspace,
                             String columnFamily,
                             String key,
                             String superColumnName )
    //public List<ColumnOrSuperColumn> getSlice(String keyspace, String columnFamily, String key, String superColumnName)
        throws CassandraException
    {
        if( superColumnName == null )
        {
            return getSlice( keyspace, columnFamily, key, (byte[]) null );
        }
        else
        {
            return getSlice( keyspace, columnFamily, key, superColumnName.getBytes() );
        }
    }

    // Yes super column -- bytes version
    // public List<ColumnOrSuperColumn> getSlice(String keyspace, String columnFamily, String key, byte[] superColumnNam)
    public HashMap getSlice( String keyspace,
                             String columnFamily,
                             String key,
                             byte[] superColumnName )
    //throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException, IllegalAccessException, NotFoundException, InstantiationException, ClassNotFoundException
        throws CassandraException
    {

        try
        {
            SliceRange range = new SliceRange( new String().getBytes(),
                                               new String().getBytes(),
                                               true,
                                               1000000 );
            
            List<ColumnOrSuperColumn> columns = null;

            columns = thriftClient.get_slice( keyspace,
                                              key, 
                                              createColumnParent(columnFamily, superColumnName),
                                              createSlicePredicate(null, range), ConsistencyLevel.ONE );

            //
            // Our return object
            //            
            HashMap map = new HashMap();

            //
            // Put the slice into a hashmap we can return
            //
            for( ColumnOrSuperColumn cosc : columns )
            {

                //
                // If this is a super column, we need to recurse
                //
                if( cosc.isSetSuper_column() )
                {

                    SuperColumn superColumn = cosc.super_column;

                    //
                    // This hashmap holds the sc->c mapping
                    //
                    HashMap submap = new HashMap();

                    //
                    // Iterate through the sub column
                    for( Column col : superColumn.getColumns() )
                    {
                        submap.put( new String( col.name ), col.value);
                    }

                    //
                    // Stick this sub mapping into out return object
                    //
                    map.put( new String( superColumn.name ), submap );

                }
                else
                {
                    //
                    // Insert a simple name/value pair into the result
                    //
                    Column column = cosc.column;

                    map.put( new String( column.name ), column.value );

                }
            }
            
            return map;

        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception getting slice " +
                                          keyspace + "|" +
                                          columnFamily + "|" +
                                          key + "|" +
                                          superColumnName + ": " +
                                          e.getMessage() );

        }

    }

    
    // No supercolumn -- string version
    // The 'value' data payload should still be in
    // bytes to avoid weird string converions issues
    public void insert( String keyspace,
                        String columnFamily,
                        String key,
                        String columnName,
                        byte[] value )
    //        throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
        throws CassandraException
    {
        insert( keyspace, columnFamily, key, columnName.getBytes(), value );
    }

    // No supercolumn -- bytes version
    public void insert( String keyspace,
                        String columnFamily,
                        String key,
                        byte[] columnName,
                        byte[] value )
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
        throws CassandraException
    {
        insert( keyspace, columnFamily, key, null, columnName, value );
    }

    // Yes supercolumn -- string version
    public void insert( String keyspace,
                        String columnFamily,
                        String key,
                        String superColumnName,
                        String columnName,
                        byte[] value )
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
        throws CassandraException
    {
        if( superColumnName == null )
        {
            insert( keyspace, columnFamily, key, (byte[]) null, columnName.getBytes(), value );
        }
        else
        {
            insert( keyspace, columnFamily, key, superColumnName.getBytes(), columnName.getBytes(), value );
        }

    }

    // Yes supercolumn -- bytes version
    public void insert( String keyspace,
                        String columnFamily,
                        String key,
                        byte[] superColumnName,
                        byte[] columnName,
                        byte[] value )
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
        throws CassandraException
    {
        try
        {

            ColumnPath columnPath = createColumnPath(columnFamily, superColumnName, columnName);

            thriftClient.insert(keyspace, key, columnPath, value, timestampMicros(), ConsistencyLevel.ONE);

        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception inserting " +
                                          keyspace + "|" +
                                          columnFamily + "|" +
                                          key + "|" +
                                          superColumnName + "|" +
                                          columnName + ": " +
                                          e.getMessage() );

        }

    }

    public List<String> list()
        throws CassandraException
    {

        try
        {
            List<String> keyspaces = thriftClient.get_string_list_property("keyspaces");

            return keyspaces;

        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception listing keyspaces: " +
                                          e.getMessage() );

        }

    }

    public void describePrint( String keyspace )
        throws CassandraException
    {
        
        Map<String, Map<String, String>> columnFamiliesMap;

        columnFamiliesMap = describe( keyspace );

        System.out.println("Describing keyspace: " + keyspace);

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

    }

    public Map<String, Map<String, String>> describe( String keyspace ) 
        throws CassandraException
    {

        try
        {

            Map<String, Map<String, String>> columnFamiliesMap;

            columnFamiliesMap = thriftClient.describe_keyspace(keyspace);

            return columnFamiliesMap;

        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception describing " +
                                          keyspace + ": " +
                                          e.getMessage() );

        }

    }

    // No supercolumn -- string version
    public void delete(String keyspace, String columnFamily, String key, String columnName)
        throws CassandraException
    {
        delete( keyspace, columnFamily, key, null, columnName.getBytes() );
    }

    // No supercolumn -- bytes version
    public void delete(String keyspace, String columnFamily, String key, byte[] columnName)
        throws CassandraException
    {
        delete( keyspace, columnFamily, key, (byte[])null, columnName );
    }

    // Yes supercolumn -- string version
    public void delete(String keyspace, String columnFamily, String key, String superColumnName, String columnName)
        throws CassandraException
    {
        delete( keyspace, columnFamily, key, superColumnName.getBytes(), columnName.getBytes() );
    }

    // Yes supercolumn -- bytes version
    public void delete(String keyspace, String columnFamily, String key, byte[] superColumnName, byte[] columnName)
    //throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
        throws CassandraException
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
            
            thriftClient.remove(keyspace, key, columnPath, timestampMicros(), ConsistencyLevel.ONE);
         
        }
        catch( Exception e )
        {

            throw new CassandraException( "Exception deleting " +
                                          keyspace + "|" +
                                          columnFamily + "|" +
                                          key + "|" +
                                          superColumnName + "|" +
                                          columnName + ": " +
                                          e.getMessage() );

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