package org.orienteer.camel.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.tools.apt.helper.Strings;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;


/**
 * Camel component for OrientDB.
 */
@UriEndpoint(scheme = "orientdb", syntax = "orientdb:sqlQuery", title = "OrientDB", label = "database,sql,orientdb") 
public class OrientDBEndpoint extends DefaultEndpoint {

	private static OPartitionedDatabasePoolFactory dbPool = new OPartitionedDatabasePoolFactory(); 
	
	private Map<String, Object> parameters;

	@UriPath(description = "Sets the query to execute") @Metadata(required = "false")
	private String sqlQuery;
	
	@UriParam(enums = "map,object,json,list", defaultValue = "map")
	private OrientDBCamelDataType outputType = OrientDBCamelDataType.map;

	@UriParam
	private String fetchPlan;
	
	@UriParam(defaultValue = "0")
	private int maxDepth = 0;

	@UriParam(defaultValue = "true")
	private boolean fetchAllEmbedded = true;
	
	@UriParam(label = "consumer")
	private String inputAsOClass;

	@UriParam(defaultValue = "false",label = "consumer")
	private boolean preload = false;

	@UriParam(defaultValue = "true",label = "consumer")
	private boolean makeNew = true;

	@UriParam(defaultValue = "rid")
	private String recordIdField = "rid";

	@UriParam(defaultValue = "class")
	private String classField = "class";

	//catalogs
	@UriParam(defaultValue = "orienteer.prop.name")
	private String catalogsLinkAttr = "orienteer.prop.name";
	
	@UriParam(defaultValue = "name")
	private String catalogsLinkName = "name";
	
	@UriParam(defaultValue = "true")
	private boolean catalogsUpdate = true;

	protected OrientDBEndpoint(String endpointUri,Component component,String remaining, Map<String, Object> parameters ) {
		super(endpointUri,component);
		this.sqlQuery = remaining;
		this.parameters = parameters;
    }

	protected OrientDBEndpoint(String endpointUri, Component component) {
		super(endpointUri,component);
    }
	
	@Override
	public Producer createProducer() throws Exception {

		return new OrientDBProducer(this);
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {

		return new OrientDBConsumer(this, processor);
	}

	@Override
	public boolean isSingleton() {

		return false;
	}
	
	public String getSQLQuery(){
		return sqlQuery;
	}
	
	public Map<String, Object> getParameters(){
		return parameters;
	}
	
	
	//should be called to open new connection
	//@SuppressWarnings("resource")
	public ODatabase<?> databaseOpen() throws Exception{
		String url = getCamelContext().getGlobalOption(OrientDBComponent.DB_URL);
		String username = getCamelContext().getGlobalOption(OrientDBComponent.DB_USERNAME);
		String password = getCamelContext().getGlobalOption(OrientDBComponent.DB_PASSWORD);
		
		if(url!=null && username!=null) {
			return dbPool.get(url, username, password).acquire();
		}else{
			throw new Exception("Cannot connect to OrientDB server without properties "+OrientDBComponent.DB_URL+" and "+OrientDBComponent.DB_USERNAME);
		}
	}
	
	//should be called to close existing connection
	public void databaseClose(ODatabase<?> db){
		db.close();
	}
	
	public Object makeOutObject(Object rawOut) throws Exception{
		if (rawOut instanceof ODocument){
			return getOutFromODocument((ODocument)rawOut);
		}else if (rawOut instanceof Iterable){
			List<Object> resultArray = new ArrayList<Object>();
			Iterable<?> tmpset = (Iterable<?>) rawOut;
			for (Object object : tmpset) {
				if (object instanceof ODocument){
					resultArray.add(getOutFromODocument((ODocument)object));
				//}else if(object instanceof OIdentifiable){
				//	resultArray.add(((OIdentifiable)object).getIdentity().toString());
				}else{
					throw new Exception("Unknown type of OrientDB object:"+object.getClass());
				}
			}
			if (outputType.equals(OrientDBCamelDataType.json)){
				if(resultArray == null || resultArray.isEmpty()) return "[]";
				else {
					StringBuilder sb = new StringBuilder(128);
					sb.append("[");
					for(Object obj : resultArray) {
						sb.append(obj).append(",");
					}
					sb.setLength(sb.length()-1);
					sb.append("]");
					return sb.toString();
				}
			}else{
				return resultArray;
			}
		}else{
			return rawOut;
		}
	}
	
	private Object getOutFromODocument(ODocument doc) throws Exception{
		if (outputType.equals(OrientDBCamelDataType.map)){
			return toMap(doc);
		}else if (outputType.equals(OrientDBCamelDataType.json)){
			return toJSON(doc);//doc.toJSON("fetchPlan:"+getFetchPlan()));
		}else if (outputType.equals(OrientDBCamelDataType.object)){
			return toObject(doc);
		}else if (outputType.equals(OrientDBCamelDataType.list)){
			return toList(doc);
		}else{
			throw new Exception("Unknown outputType :"+outputType.toString());
		}
	} 
	
	
	private Object toJSON(ODocument obj){
		if (Strings.isNullOrEmpty(getFetchPlan())){
			return obj.toJSON();
		}else{
			return obj.toJSON("fetchPlan:"+getFetchPlan());
		}
	}
	
	private Object toObject(Object obj){
		return obj;
	}
	
	//this method save only fields,selected by user as flat list
	private Object toList(Object obj){
		if (obj instanceof ODocument){
			ODocument objDoc =(ODocument)obj;
			List<Object> result = new ArrayList<Object>();

			for (Object value : objDoc.fieldValues()){
		    	if (value instanceof ODocument){
			    	result.add(((ODocument) value).toJSON());
		    	}else{
			    	result.add(value.toString());
		    	}
		    }
    		return result;
		}else{
			return obj;
		}	
	}

	private Object toMap(Object obj){
		return toMap(obj,0);
	}
	
	private Object toMap(Object obj,int depth){
		if (obj instanceof ODocument){
			ODocument objDoc =(ODocument)obj; 
			Map<String,Object> result = new HashMap<String,Object>();
			if((objDoc.isEmbedded()&& isFetchAllEmbedded()) || (depth<=getMaxDepth())){
			    for (String fieldName : objDoc.fieldNames()){
			    	result.put(fieldName, toMap(objDoc.field(fieldName),depth+1));
			    }
			}
		    final ORID id = objDoc.getIdentity();
		    if (id.isValid() && id.isPersistent() )
		    	result.put(getRecordIdField(), id.toString());

			final String className = objDoc.getClassName();
			if (className != null)
				result.put(getClassField(), className);
			return result;
		}else if(obj instanceof Map){
    		Map<String,Object> result = new HashMap<String,Object>();
    		Map<?, ?> source = (Map<?,?>)obj;
    		for (Entry<?, ?> entry : (source).entrySet()) {
   				result.put((String) entry.getKey(),toMap(entry.getValue(),depth+1));	
			}
    		return result;
		}else if(obj instanceof Iterable){
    		List<Object> result = new ArrayList<Object>();
    		for (Object subfield : (Iterable<?>)obj) {
    			result.add(toMap(subfield,depth+1));	
			}
    		return result;
		}else{
			return obj;
		}
	}
	
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////


	public String getFetchPlan() {
		return fetchPlan;
	}

	/**
	 * Set fetch plan (view orientdb documentation, like http://orientdb.com/docs/2.0/orientdb.wiki/Fetching-Strategies.html)
	 * @param fetchPlan - fetch plan to be used
	 */
	public void setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
	}

	public String getInputAsOClass() {
		return inputAsOClass;
	}

	/**
	 * Rewrite "@class" field value in root document(s) 
	 * @param inputAsOClass - OClass to be used for root document
	 */
	public void setInputAsOClass(String inputAsOClass) {
		this.inputAsOClass = inputAsOClass;
	}

	public boolean isPreload() {
		return preload;
	}

	/**
	 * Save ODocument from input data BEFORE query  
	 * @param preload - if true - ODocument will be saved 
	 */
	public void setPreload(boolean preload) {
		this.preload = preload;
	}

	public boolean isMakeNew() {
		return makeNew;
	}

	/**
	 * Clear ODocuments RID`s in PRELOAD phase BEFORE save
	 * @param makeNew - should ODocument created from scratch to be reused
	 */
	public void setMakeNew(boolean makeNew) {
		this.makeNew = makeNew;
	}

	public OrientDBCamelDataType getOutputType() {
		return outputType;
	}

	/**
	 * Output data type of single row.  
	 * @param outputType - type of the output. Check {@link OrientDBCamelDataType} for details
	 */
	public void setOutputType(OrientDBCamelDataType outputType) {
		this.outputType = outputType;
	}

	public int getMaxDepth() {
		return maxDepth;
	}

	/**
	 * Max fetch depth. Only for "map" type 
	 * @param maxDepth - depth of a recurrent conversions to map
	 */
	public void setMaxDepth(int maxDepth) {
		this.maxDepth = maxDepth;
	}

	public boolean isFetchAllEmbedded() {
		return fetchAllEmbedded;
	}

	/**
	 * Fetch all embedded(not linked) objects, ignore "maxDepth". Only for "map" type. 
	 * @param fetchAllEmbedded - if true - embedded documents will be also converted to maps
	 */
	public void setFetchAllEmbedded(boolean fetchAllEmbedded) {
		this.fetchAllEmbedded = fetchAllEmbedded;
	}

	public String getRecordIdField() {
		return recordIdField;
	}

	/**
	 * Your "@rid" renamed to recordIdField value  
	 * @param recordIdField - name of the field for RID of a document. Default value is "rid"
	 */
	public void setRecordIdField(String recordIdField) {
		this.recordIdField = recordIdField;
	}

	public String getClassField() {
		return classField;
	}

	/**
	 * Your "@class" renamed to classField value 
	 * @param classField - name of the field for OClass of a document. Default value is "class" 
	 */
	public void setClassField(String classField) {
		this.classField = classField;
	}

	public String getCatalogsLinkAttr() {
		return catalogsLinkAttr;
	}

	/**
	 * Name of your custom attribute,
	 * linked to catalog class and containing name of field in this class,
	 * associated to name of element of this class 
	 * @param catalogsLinkAttr - name of property on a linkedClass which will be used for mapping
	 */
	public void setCatalogsLinkAttr(String catalogsLinkAttr) {
		this.catalogsLinkAttr = catalogsLinkAttr;
	}

	public String getCatalogsLinkName() {
		return catalogsLinkName;
	}

	/**
	 * If you not use "catalogsLinkAttr",
	 * you can set field name of "name" element in catalogs class directly here
	 * @param catalogsLinkName - name of property of linkedClass which will be used for mapping
	 */
	public void setCatalogsLinkName(String catalogsLinkName) {
		this.catalogsLinkName = catalogsLinkName;
	}

	public boolean isCatalogsUpdate() {
		return catalogsUpdate;
	}

	/**
	 * If you set "catalogsLinkAttr" or "catalogsLinkName", catalogs may be autoupdated,
	 * if "catalogsUpdate" = true.
	 * If is that - in catalogs classes may be created empty elements with new "name" field values.
	 * @param catalogsUpdate - if yes - final catalog can be updated per data which being loaded
	 */
	
	public void setCatalogsUpdate(boolean catalogsUpdate) {
		this.catalogsUpdate = catalogsUpdate;
	}




}
