package org.orienteer.camel.component;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.wicket.util.string.Strings;
import org.junit.Test;

public class OrientDBComponentTest extends CamelTestSupport {

    @Test
    public void testOrientDB() throws Exception {
        //MockEndpoint mock = getMockEndpoint("mock:result");
        //mock.expectedMinimumMessageCount(1);       
        
        //assertMockEndpointsSatisfied();
    	//log.error(OrientDBCamelDataType.);
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                //from("orientdb://foo")
                //  .to("orientdb://bar")
                //  .to("mock:result");
            }
        };
    }
}
