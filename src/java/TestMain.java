
import scc.*;

import java.util.List;

// Thrift imports
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;


public class TestMain
{
    public static void main(String args[])
    {

        SimpleCassandraClient scc = new SimpleCassandraClient();
        scc.connect( "localhost", 9160 );

        testDel(scc);
        //testMktStk();

        scc.disconnect();

    }
    
    public static void testDel( SimpleCassandraClient scc )
    {

        byte[] value;

        if( scc.insert("Control", "MktStk", "TickString", "AAPL", "AAPL".getBytes()) )
        {
            System.out.println("Insert succeeded");
        }
        else
        {
            System.out.println("Insert failed");
        }

        if( (value = scc.get("Control", "MktStk", "TickString", "AAPL")) != null )
        {
            System.out.println( "Value: " + new String(value) );
        }
        else
        {
            System.out.println("Get failed");
        }



        if( scc.delete("Control", "MktStk", "TickString", "AAPL") )
        {
            System.out.println("Delete succeeded");
        }
        else
        {
            System.out.println("Delete failed");
        }

        if( (value = scc.get("Control", "MktStk", "TickString", "AAPL")) != null )
        {
            System.out.println( "Value: " + new String(value) );
        }
        else
        {
            System.out.println("Get failed");
        }


    }

    public void testMktStk( SimpleCassandraClient scc )
    {

        byte[] value;

        scc.list();

        scc.describe("Control");

        if( (value = scc.get("Control", "MktStk", "TickString","AAPL")) != null )
        {
            System.out.println( "Value: " + new String(value) );
        }
        else
        {
            System.out.println("Get failed");
        }

        if( (value = scc.get("Control", "MktStk", "TickString", "ATVI")) != null )
        {
            System.out.println( "Value: " + new String(value) );
        }
        else
        {
            System.out.println("Get failed");
        }
        
        List<ColumnOrSuperColumn> lvalue;

        if( (lvalue = scc.getSlice("Control", "MktStk", "TickString")) != null )
        {
            System.out.println( "Value: " + new Integer(lvalue.size()).toString() );
        }
        else
        {
            System.out.println("Get failed");
        }

        if( (lvalue = scc.getSlice("MktStk", "TickString", "AAPL", "20100702")) != null )
        {
            System.out.println( "Value: " + new Integer(lvalue.size()).toString() );
        }
        else
        {
            System.out.println("Get failed");
        }

    }

    public void test( SimpleCassandraClient scc )
    {
        System.out.println("SCC test client");

        scc.connect( "localhost", 9160 );

        if( scc.insert("Keyspace1", "Standard2", "aapl", "2010", "250.0".getBytes()) )
        {
            System.out.println("Insert succeeded");
        }
        else
        {
            System.out.println("Insert failed");
        }

        if( scc.insert("Keyspace1", "Super1", "aapl", "12:00", "2010", "250.0".getBytes()) )
        {
            System.out.println("Insert super succeeded");
        }
        else
        {
            System.out.println("Insert super failed");
        }

        byte[] value;

        if( (value = scc.get("Keyspace1", "Standard2", "aapl", "2010")) != null )
        {
            System.out.println( new String(value) );
        }
        else
        {
            System.out.println("Get failed");
        }

        if( (value = scc.get("Keyspace1", "Super1", "aapl", "12:00", "2010")) != null )
        {
            System.out.println( new String(value) );
        }
        else
        {
            System.out.println("Get super failed");
        }

    }

}
