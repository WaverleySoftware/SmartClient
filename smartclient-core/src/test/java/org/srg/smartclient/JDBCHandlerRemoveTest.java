package org.srg.smartclient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.srg.smartclient.isomorphic.DSRequest;
import org.srg.smartclient.isomorphic.DSResponse;
import org.srg.smartclient.jpa.Client;
import org.srg.smartclient.utils.ContextualRuntimeException;
import org.srg.smartclient.utils.Serde;

import java.io.StringWriter;

import static org.srg.smartclient.AbstractJDBCHandlerTest.ExtraField.Deleted;

public class JDBCHandlerRemoveTest extends AbstractJDBCHandlerTest<JDBCHandler> {

    @Override
    protected Class<JDBCHandler> getHandlerClass() {
        return JDBCHandler.class;
    }

    @Test
    public void simpleSoftDelete() throws Exception {

        final DSRequest request = JsonTestSupport.fromJSON(new TypeReference<>(){}, """
               {
                 useStrictJSON : true,
                 dataSource : "EmployeeDS",
                 operationType : "REMOVE",
                 textMatchStyle : "EXACT",
                 data : {
                   id: 1,
                   _metadata: "string"
                 },
                 oldValues : null
               }                
            """);

        final DSResponse response;

        DSResponse newResp;

        try {
            handler = withExtraFields(Deleted);
            handler.handleRemove(request);
            DSRequest request2 = new DSRequest();
            response = handler.handleFetch(request2);
        } catch (ContextualRuntimeException e) {
            /*
             * This exception handler can be used  to check and adjust
             * ContextualRuntimeException.dumpContext_ifAny().
             *
             * Other than that it does not have any sense
             */
            final StringWriter sw = new StringWriter();
            final ObjectMapper mapper = Serde.createMapper();

            e.dumpContext_ifAny(sw, "  ", mapper.writerWithDefaultPrettyPrinter());
            System.out.println( sw.toString());
            throw new RuntimeException(e);
        }
        JsonTestSupport.assertJsonEquals("""
                 {
                     status: 0,
                     startRow: 0,
                     endRow: 5,
                     totalRows: 5,
                     data:[
                         {
                             id:2,
                             name: 'developer',
                             deleted: false
                         },
                         {
                             id:3,
                             name: 'UseR3',
                             deleted: false
                         },
                         {
                             id:4,
                             name: 'manager1',
                             deleted: false
                         },
                         {
                             id:5,
                             name: 'manager2',
                             deleted: false
                         },
                         {
                             id:6,
                             name: 'user2',
                             deleted: false
                         }
                     ]
                }""", response);
    }

    @Test
    public void simpleDelete() throws Exception {

        final DSRequest request = JsonTestSupport.fromJSON(new TypeReference<>(){}, """
               {
                 useStrictJSON : true,
                 dataSource : "CountryDS",
                 operationType : "REMOVE",
                 textMatchStyle : "EXACT",
                 data : {
                   id: 1,
                   _metadata: "string"
                 },
                 oldValues : null
               }                
            """);

        final DSResponse response;

        DSResponse newResp;

        try {
            handler = withHandlers(Handler.ClientData);
            handler.handleRemove(request);
            DSRequest request2 = new DSRequest();
            response = handler.handleFetch(request2);
        } catch (ContextualRuntimeException e) {
            /*
             * This exception handler can be used  to check and adjust
             * ContextualRuntimeException.dumpContext_ifAny().
             *
             * Other than that it does not have any sense
             */
            final StringWriter sw = new StringWriter();
            final ObjectMapper mapper = Serde.createMapper();

            e.dumpContext_ifAny(sw, "  ", mapper.writerWithDefaultPrettyPrinter());
            System.out.println( sw.toString());
            throw new RuntimeException(e);
        }
        JsonTestSupport.assertJsonEquals("""
                 {
                     status: 0,
                     startRow: 0,
                     endRow: 1,
                     totalRows: 1,
                     data:[
                         {
                             id:2,
                             data: 'Data2: client 2'
                         }
                     ]
                }""", response);
    }



}
