package org.srg.smartclient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.srg.smartclient.isomorphic.DSRequest;
import org.srg.smartclient.isomorphic.DSResponse;
import org.srg.smartclient.utils.ContextualRuntimeException;
import org.srg.smartclient.utils.Serde;

import java.io.StringWriter;

public class JDBCHandlerAddTest extends AbstractJDBCHandlerTest<JDBCHandler> {

    @Override
    protected Class<JDBCHandler> getHandlerClass() {
        return JDBCHandler.class;
    }

    @Test
    public void simpleAdd() throws Exception {

        final DSRequest request = JsonTestSupport.fromJSON(new TypeReference<>(){}, """
               {
                 useStrictJSON : true,
                 dataSource : "EmployeeDS",
                 operationType : "ADD",
                 textMatchStyle : "EXACT",
                 data : {
                   name: "A new Record"
                 },
                 oldValues : null
               }                
            """);

        final DSResponse response;

        try {
            response = handler.handleAdd(request);
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
                     data:[
                         {
                             id:7,
                             name: 'A new Record'
                         }
                     ]
                }""", response);
    }

    @Test
    public void addingWithMultipleJoin() throws Exception {

        handler =  withHandlers(Handler.Client, Handler.Project);
        withExtraFields(handler, ExtraField.Project_IncludeFromClient);

        final DSRequest request = JsonTestSupport.fromJSON(new TypeReference<>(){}, """

                   {
                        useStrictJSON : true,
                        operationType : "ADD",
                        textMatchStyle : "EXACT",
                        dataSource: 'ProjectDS',
                        data:
                             {
                                id: "6",
                                name:"New Project",
                                client: 1,
                                clientName: "client 1"
                             },
                        oldValues : null

                   }
                """);

        final DSResponse response;

        try {
            response = handler.handleAdd(request);
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
                     data:[
                         {
                             id:6,
                             name:"New Project",
                             client: 1,
                             clientName: "client 1"
                         }
                     ]
                }""", response);
    }

    @Test
    public void addingMustFails_if_providedIdAlreadyExists() throws Exception {
        withExtraFields(ExtraField.Email);

        final DSRequest request = JsonTestSupport.fromJSON(new TypeReference<>(){}, """
               {
                 useStrictJSON : true,
                 dataSource : "EmployeeDS",
                 operationType : "ADD",
                 textMatchStyle : "EXACT",
                 data : {
                    id: 2,
                   name: 'A new Record'
                 },
                 oldValues : null
               }
            """);


        final Exception ex = Assertions.assertThrows(Exception.class, () -> {
            handler.handleAdd(request);
        });


        Assertions.assertTrue( ex.getMessage().contains("SQL add query execution failed."));
    }


}
