package org.orienteer.camel.component;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

public class OrientDBComponentTest extends CamelTestSupport {

	private static final String DB_NAME = "CamelOrientdbTest";
	private static final String DB_URL = "memory:"+DB_NAME;
	private static final String DB_USERNAME = "admin";
	private static final String DB_PASSWORD = "admin";
	private static final String TEST_CLASS = "CamelOrientdbTestClass";
	private static final String TEST_PROPERTY = "prop";
	private static final String TEST_PROPERTY_VALUE = "propValue";
	private static final String TEST_LINK_PROPERTY = "link";
	private static final String TEST_LINKED_CLASS = "CamelOrientdbTestClassLinked";
	private OServer server;
	
		
	@Override
	protected void doPreSetup() throws Exception {
		// TODO Auto-generated method stub
        server = OServerMain.create();
        server.startup(getClass().getResourceAsStream("db.config.xml"));
        server.activate();
        //server.
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(DB_URL,false);
		db = db.create();
		db.command(new OCommandSQL("CREATE CLASS "+TEST_CLASS)).execute();
		db.command(new OCommandSQL("CREATE PROPERTY "+TEST_CLASS+"."+TEST_PROPERTY+" STRING")).execute();
		db.command(new OCommandSQL("CREATE PROPERTY "+TEST_CLASS+"."+TEST_LINK_PROPERTY+" LINK")).execute();
		db.command(new OCommandSQL("CREATE CLASS "+TEST_LINKED_CLASS)).execute();
		db.command(new OCommandSQL("CREATE PROPERTY "+TEST_LINKED_CLASS+"."+TEST_PROPERTY+" STRING")).execute();
		//db.command(new OCommandSQL("INSERT INTO "+TEST_LINKED_CLASS+" SET "+TEST_PROPERTY+"=\""+TEST_PROPERTY_VALUE+"\"")).execute();
		
		db.close();
        Thread.sleep(1000);
		super.doPreSetup();
	}
	
    @Test
    public void testOrientDBfinalize() throws Exception {
        Thread.sleep(2000);
        
    	ODatabaseDocumentTx db = new ODatabaseDocumentTx(DB_URL).open(DB_USERNAME, DB_PASSWORD);
    	List<ODocument> dbResult = db.query(new OSQLSynchQuery<ODocument>("SELECT FROM "+TEST_CLASS));
    	
    	assertEquals((int)2, dbResult.size());
    	assertNotNull(dbResult.get(1).field(TEST_LINK_PROPERTY));
    	
        db.close();
        server.shutdown();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
    			Map<String, String> properties = context.getProperties();
    			properties.put(OrientDBComponent.DB_URL, DB_URL);
    			properties.put(OrientDBComponent.DB_USERNAME, DB_USERNAME);
    			properties.put(OrientDBComponent.DB_PASSWORD, DB_PASSWORD);
    			context.setProperties(properties);

    			from("orientdb:INSERT INTO "+TEST_LINKED_CLASS+" SET "+TEST_PROPERTY+"=\""+TEST_PROPERTY_VALUE+"\"?outputType=map&preload=false")
    			.to("orientdb:INSERT INTO "+TEST_CLASS+" SET "+TEST_PROPERTY+"=\""+TEST_PROPERTY_VALUE+"\", "+TEST_LINK_PROPERTY+"=:rid")
    			.to("orientdb:?preload=true&makeNew=true")
    			
                //  .to("orientdb://bar")
                	.to("mock:result");
            }
        };
    }
}
