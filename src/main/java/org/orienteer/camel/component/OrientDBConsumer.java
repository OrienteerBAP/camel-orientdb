package org.orienteer.camel.component;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

/**
 * Consumer(from) for {@link OrientDBEndpoint}  
 */
public class OrientDBConsumer extends DefaultConsumer{

	public OrientDBConsumer(Endpoint endpoint, Processor processor) {
		super(endpoint, processor);
		
	}

	@Override
	protected void doStart() throws Exception {
		OrientDBEndpoint endpoint = (OrientDBEndpoint)getEndpoint();
		ODatabase<?> db = endpoint.databaseOpen();
		Object dbResult = db.command(new OCommandSQL(endpoint.getSQLQuery())).execute();
		
		Object out = endpoint.makeOutObject(dbResult);
		endpoint.databaseClose(db);
		
		Exchange exchange = getEndpoint().createExchange();
		exchange.getIn().setBody(out);
		getProcessor().process(exchange);
		
		super.doStart();
	}
}
