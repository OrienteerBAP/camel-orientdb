package org.orienteer.camel.component;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.tools.apt.helper.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultProducer;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 *	Producer for {@link OrientDBEndpoint} 
 *
 */

public class OrientDBProducer extends DefaultProducer{

	private ODatabase<?> curDb;
	public OrientDBProducer(Endpoint endpoint) {
		super(endpoint);
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		OrientDBEndpoint endpoint = (OrientDBEndpoint)getEndpoint();
		curDb = endpoint.databaseOpen();
		Object input = exchange.getIn().getBody();
		Message out = exchange.getOut(); 
		out.getHeaders().putAll(exchange.getIn().getHeaders());

		
		if (input instanceof List){
			out.setBody(endpoint.makeOutObject(processList((List<?>)input, endpoint, curDb)));
		}else if (input instanceof String && isJSONList((String)input)){
			List<String> inputList =  strToJSONsList((String)input);
			out.setBody(endpoint.makeOutObject(processList(inputList, endpoint, curDb)));
		}else{
			out.setBody(endpoint.makeOutObject(processSingleObject(input, endpoint, curDb)));
		}
		endpoint.databaseClose(curDb);
		curDb=null;
	}
	
	private boolean isJSONList(String input){
		return input.matches("^\\[\\{.+\\}\\]$");
	}
	
	private boolean isJsonObject(String input){
		return input.matches("^\\{.*\\}$");
	}
	
	private List<String> strToJSONsList(String str){
		ArrayList<String> result = new ArrayList<String>();
		double bracketCounter = 0;
		double startBracket = -1;
		char openb = '{';
		char closeb = '}';
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i)==openb){
				bracketCounter++;
				if (startBracket==-1){
					startBracket=i;
				}
			}else if(str.charAt(i)==closeb){
				bracketCounter--;
				if (startBracket!=-1 && bracketCounter==0){
					result.add(str.substring((int)startBracket, i+1));
					startBracket=-1;
				}
			}
		}
		return result;		
	}
	
	private List<Object> processList(List<?> inputList,OrientDBEndpoint endpoint,ODatabase<?> db) throws Exception{
		List<Object> outputList = new ArrayList<Object>();
		for (Object inputElement : inputList) {
			Object dbResult = processSingleObject(inputElement,endpoint,db);
			if (dbResult instanceof List){
				outputList.addAll((List<?>)dbResult);
			}else{
				outputList.add(dbResult);
			}
		}
		return outputList;
	}
	
	@SuppressWarnings("unchecked")
	private Object processSingleObject(Object input,OrientDBEndpoint endpoint,ODatabase<?> db) throws Exception{
		ODocument inputDocument = null;
		if (input instanceof Map){
			if (!Strings.isNullOrEmpty(endpoint.getInputAsOClass())){
				((Map<Object,Object>)input).put(getOrientDBEndpoint().getClassField(),endpoint.getInputAsOClass());
			}
			inputDocument = (ODocument) fromMap(input);
		}else if(input instanceof ODocument){
			inputDocument = fromObject((ODocument)input, endpoint, db);
		}else if(input instanceof String && isJsonObject((String)input)){
			inputDocument = fromJSON((String)input, endpoint, db);
		}
		if (inputDocument!=null){
			if (!Strings.isNullOrEmpty(endpoint.getInputAsOClass())){
				inputDocument.setClassName(endpoint.getInputAsOClass());
			}
			if(endpoint.isPreload()){
				if (endpoint.isMakeNew()){
					inputDocument.getIdentity().reset();
				}
				inputDocument.save();
			}
			if (!Strings.isNullOrEmpty(endpoint.getSQLQuery())){
				Map<String, Object> tmp = toParamMap(inputDocument);
				Object dbResult = db.command(new OCommandSQL(endpoint.getSQLQuery())).execute(tmp);
				return dbResult;
			}
			return inputDocument;
		}else{
			if (!Strings.isNullOrEmpty(endpoint.getSQLQuery())){
				if (input instanceof List){
					convertLinks((List<Object>)input);//without this method links assignment does not work
					Object dbResult = db.command(new OCommandSQL(endpoint.getSQLQuery())).execute(((List<?>)input).toArray());
					return dbResult;
				}else{
					Object dbResult = db.command(new OCommandSQL(endpoint.getSQLQuery())).execute(input);
					return dbResult;
				}
			}
		}
		return input;
	}
	
	private void convertLinks(List<Object> input){
		for (int i = 0; i < input.size(); i++) {
			if (input.get(i).toString().matches("^#[\\d]+:[\\d]+$")){
				input.set(i, new ORecordId(input.get(i).toString()));
			}
		}
	}
	
	private Object convertToCatalogLinkIfAble(Object value,OProperty fieldProperty,ODocument mainDoc){
		String catalogsLinkNameAttribute = getOrientDBEndpoint().getCatalogsLinkAttr();//
		String catalogsLinkName = getOrientDBEndpoint().getCatalogsLinkName();//
		String catalogNameField = fieldProperty.getLinkedClass().getCustom(catalogsLinkNameAttribute); 
		if (catalogNameField==null){
			catalogNameField = catalogsLinkName;
		}
		List<OIdentifiable> catalogLinks = curDb.query(new OSQLSynchQuery<OIdentifiable>(
				"select from "+fieldProperty.getLinkedClass().getName()+" where "+catalogNameField+"=?"), value);
		if (catalogLinks.size()>0){
			value = catalogLinks.get(0).getIdentity();
		}else{
			boolean updateCatalogs = getOrientDBEndpoint().isCatalogsUpdate();//
			if (updateCatalogs){
				ODocument catalogRecord = new ODocument(fieldProperty.getLinkedClass());
				catalogRecord.field(catalogNameField,value);
				catalogRecord.save(true);
				value = catalogRecord.getIdentity();
			}
		}
		return value;
	}
	
	private Object fromMap(Object input){
		if (input instanceof Map){//something like ODocument
			Map<?, ?> objMap = (Map<?,?>)input;
			String rid = (String)(objMap.remove(getOrientDBEndpoint().getRecordIdField()));
			String clazz = (String)(objMap.remove(getOrientDBEndpoint().getClassField()));
			if (rid!=null || clazz!=null){
				ODocument result=null;
				if (rid!=null && clazz!=null && objMap.isEmpty()){ //it is document link
					result = new ODocument(clazz,new ORecordId(rid));
				}else if (clazz!=null &&
						(	rid==null || 
							((OrientDBEndpoint)getEndpoint()).isMakeNew() && ((OrientDBEndpoint)getEndpoint()).isPreload()
						)
					){//it is embedded or new document
					result = new ODocument(clazz);
				}else if (rid!=null && clazz!=null){ //it is document
					result = new ODocument(clazz,new ORecordId(rid));
				}else if (rid!=null){//it is something like broken link  
					result = new ODocument(new ORecordId(rid));
				}
				for (Entry<?, ?> entry : objMap.entrySet()) {
					Object value = fromMap(entry.getValue());
					String fieldName = (String) entry.getKey();
					if (value instanceof String){
						if (Strings.isNullOrEmpty((String)value)){
							value=null;//clear empty strings
						}else{
							if (result.getSchemaClass()!=null){
								OProperty fieldProperty = result.getSchemaClass().getProperty(fieldName);
								if (fieldProperty!= null && OType.LINK.equals(fieldProperty.getType())){
									value = convertToCatalogLinkIfAble(value, fieldProperty, result);	
								}								
							}					
						}
					}

					result.field(fieldName,value);
				}
				return result;
			}else{//wow,it is just Map
				Map<String,Object> result = new HashMap<String,Object>();
				for (Entry<?, ?> entry : objMap.entrySet()) {
					result.put((String) entry.getKey(),fromMap(entry.getValue()));
				}
				return result;
			}
		}else if (input instanceof Iterable){//something like list
			ArrayList<Object> result = new ArrayList<Object>(); 
			for (Object item : ((Iterable<?>)input)) {
				result.add(fromMap(item));
			}
			return result;
		}
		return input;		
	}

	private ODocument fromJSON(String input,OrientDBEndpoint endpoint,ODatabase<?> db){
		ODocument out = new ODocument();
		out.fromJSON(input);
		return out;
	}

	private ODocument fromObject(ODocument input,OrientDBEndpoint endpoint,ODatabase<?> db){
		return input;
	}
	
	private Map<String, Object> toParamMap(ODocument input){
		Map<String, Object> out = input.toMap();
		if (out.containsKey(ODocumentHelper.ATTRIBUTE_RID)){
			out.put(getOrientDBEndpoint().getRecordIdField(), out.remove(ODocumentHelper.ATTRIBUTE_RID));
		}
		if (out.containsKey(ODocumentHelper.ATTRIBUTE_CLASS)){
			out.put(getOrientDBEndpoint().getClassField(), out.remove(ODocumentHelper.ATTRIBUTE_CLASS));
		}
		
		return out;
	}
	
	private OrientDBEndpoint getOrientDBEndpoint(){
		return (OrientDBEndpoint)getEndpoint();
	}
}