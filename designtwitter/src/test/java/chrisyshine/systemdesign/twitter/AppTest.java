package chrisyshine.systemdesign.twitter;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
        
        Timestamp time = new Timestamp(System.currentTimeMillis());
        System.out.println(time.getTime());
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time));
        
    }
}
