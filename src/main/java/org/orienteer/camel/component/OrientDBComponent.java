package org.orienteer.camel.component;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;

/**
 * Camel component for OrientDB. Used {@link OrientDBEndpoint}}
 */
public class OrientDBComponent extends UriEndpointComponent{

	/**
	 * Orientdb url property id 
	 */
	public static final String DB_URL="orientdb.url"; 
	/**
	 * Orientdb username property id
	 */
	public static final String DB_USERNAME="orientdb.username"; 
	/**
	 * Orientdb password property id
	 */
	public static final String DB_PASSWORD="orientdb.password"; 
	
	public OrientDBComponent() {
		super(OrientDBEndpoint.class);
	}

	public OrientDBComponent(CamelContext context) {
		super(context,OrientDBEndpoint.class);
	}
	
	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {

		OrientDBEndpoint ep = new OrientDBEndpoint(uri,this,prepareQuery(remaining),parameters);
		return ep;
	}
	
	private String prepareQuery(String uri){
		return uri.replaceAll(":#", "?");
	}
}
