
import scc.*;

// Java imports
import java.util.HashMap;
import java.util.Collection;

public class TestMain
{
    public static void main(String args[])
        throws CassandraException, Exception
    {

        SimpleCassandraClient scc = new SimpleCassandraClient();
        scc.connect( "localhost", 9160 );

        //testDel(scc);
        //testMktStk();
        testActive(scc);

        scc.disconnect();

    }

    public static void testActive( SimpleCassandraClient scc )
        throws Exception
    {

        HashMap controlColumns = new HashMap();
        
        controlColumns = scc.getSlice( "Control", "MktStk", "Active" );

        Collection<byte[]> columnNames = controlColumns.values();

        for( byte[] symbol : columnNames )
        {
            System.out.println("Column " + new String(symbol));
        }

    }

    public static void testDel( SimpleCassandraClient scc )
        throws Exception
    {

        byte[] value;

        try
        {
            scc.insert("Control", "MktStk", "TickString", "AAPL", "AAPL".getBytes());
            System.out.println("Insert succeeded");

            value = scc.get("Control", "MktStk", "TickString", "AAPL");
            System.out.println( "Value: " + new String(value) );


            scc.delete("Control", "MktStk", "TickString", "AAPL");
            System.out.println("Delete succeeded");


            try
            {
                value = scc.get("Control", "MktStk", "TickString", "AAPL");
            }
            catch( Exception e )
            {
                System.out.println( "Value not found (Success)" );
            }
        }
        catch( Exception e )
        {
            System.out.println("Failure");
            throw e;
        }

    }

    public void testMktStk( SimpleCassandraClient scc )
    {

        byte[] value;

        try
        {
            scc.list();

            scc.describe("Control");


            value = scc.get("Control", "MktStk", "TickString","AAPL");
            System.out.println( "Value: " + new String(value) );

            value = scc.get("Control", "MktStk", "TickString", "ATVI");
            System.out.println( "Value: " + new String(value) );
        
            HashMap hvalue;

            hvalue = scc.getSlice("Control", "MktStk", "TickString");
            System.out.println( "Value: " + new Integer(hvalue.size()).toString() );

            hvalue = scc.getSlice("MktStk", "TickString", "AAPL", "20100702");
            System.out.println( "Value: " + new Integer(hvalue.size()).toString() );
        }
        catch ( Exception e)
        {
            System.out.println("Failure");
        }
    }

    public void test( SimpleCassandraClient scc )
    {
        System.out.println("SCC test client");

        try
        {
            scc.connect( "localhost", 9160 );

            scc.insert("Keyspace1", "Standard2", "aapl", "2010", "250.0".getBytes());
            System.out.println("Insert succeeded");

            scc.insert("Keyspace1", "Super1", "aapl", "12:00", "2010", "250.0".getBytes());
            System.out.println("Insert super succeeded");

            byte[] value;

            value = scc.get("Keyspace1", "Standard2", "aapl", "2010");
            System.out.println( new String(value) );

            value = scc.get("Keyspace1", "Super1", "aapl", "12:00", "2010");
            System.out.println( new String(value) );
        }
        catch( Exception e )
        {
            System.out.println("Failure");
        }
    }

}
