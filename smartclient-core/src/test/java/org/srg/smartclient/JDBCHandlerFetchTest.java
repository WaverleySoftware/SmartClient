package org.srg.smartclient;

import org.junit.jupiter.api.Test;
import org.srg.smartclient.isomorphic.DSRequest;
import org.srg.smartclient.isomorphic.DSResponse;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class JDBCHandlerFetchTest extends AbstractJDBCHandlerTest<JDBCHandler> {

    @Override
    protected Class<JDBCHandler> getHandlerClass() {
        return JDBCHandler.class;
    }

    @Test
    public void fetchAll() throws Exception {
        DSRequest request = new DSRequest();
        final DSResponse response = handler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
                 {
                     status: 0,
                     startRow: 0,
                     endRow: 6,
                     totalRows: 6,
                     data:[
                         {
                             id:1,
                             name: 'admin'
                         },
                         {
                             id:2,
                             name: 'developer'
                         },
                         {
                             id:3,
                             name: 'UseR3'
                         },
                         {
                             id:4,
                             name: 'manager1'
                         },
                         {
                             id:5,
                             name: 'manager2'
                         },
                         {
                             id:6,
                             name: 'user2'
                         }
                     ]
                }""", response);
    }

    @Test
    public void fetchAllWithEmptySortBy() throws Exception {
        DSRequest request = new DSRequest();
        request.setSortBy(new LinkedList<>());
        final DSResponse response = handler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
                 {
                     status: 0,
                     startRow: 0,
                     endRow: 6,
                     totalRows: 6,
                     data:[
                         {
                             id:1,
                             name: 'admin'
                         },
                         {
                             id:2,
                             name: 'developer'
                         },
                         {
                             id:3,
                             name: 'UseR3'
                         },
                         {
                             id:4,
                             name: 'manager1'
                         },
                         {
                             id:5,
                             name: 'manager2'
                         },
                         {
                             id:6,
                             name: 'user2'
                         }
                     ]
                }""", response);
    }

    @Test
    public void fetchSpecifiedFields() throws Exception {
        withExtraFields(ExtraField.Email);

        DSRequest request = new DSRequest();
        request.setOutputs("id, email");
        final DSResponse response = handler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
            {
              status:0,
              startRow:0,
              endRow:6,
              totalRows:6,
              data:[
                 {
                    id:1,
                    email:'admin@acmE.org'
                 },
                 {
                    id:2,
                    email:'developer@acme.org'
                 },
                 {
                    id:3,
                    email:'u3@emca.org'
                 },
                 {
                    id:4,
                    email:'pm1@acmE.org'
                 },
                 {
                    id:5,
                    email:'pm2@acme.org'
                 },
                 {
                    id:6,
                    email:'u2@emca.org'
                 }
              ]
            }""", response
        );
    }


    @Test
    public void fetchPaginated() throws Exception {
        // -- the 1'st page
        DSRequest request = new DSRequest();
        request.setStartRow(0);
        request.setEndRow(2);

        final DSResponse response1 = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
                {
                    status: 0,
                    startRow: 0,
                    endRow: 2,
                    totalRows: 6,
                    data:[
                        {
                            id:1,
                            name: 'admin'
                        },
                        {
                            id:2,
                            name: 'developer'
                        }
                    ]    
                }""", response1);

        // -- the 2-nd page
        request.setStartRow(2);
        request.setEndRow(4);
        final DSResponse response2 = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
                {
                    status: 0,
                    startRow: 2,
                    endRow: 4,
                    totalRows: 6,
                    data:[
                        {
                            id:3,
                            name: 'UseR3'
                        },
                        {
                            id:4,
                            name: 'manager1'
                        }
                    ]    
                }""", response2);

        // the 3-rd page (The last one)
        request.setStartRow(4);
        request.setEndRow(6);
        final DSResponse response3 = handler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
                {
                    status: 0,
                    startRow: 4,
                    endRow: 6,
                    totalRows: 6,
                    data:[
                        {
                            id:5,
                            name: 'manager2'
                        },
                        {
                            id:6,
                            name: 'user2'
                        }
                    ]    
                }""", response3);
    }

    @Test
    public void fetchIncludeFromField() throws Exception {
        withExtraFields(ExtraField.IncludeFromLocation);
        withHandlers(Handler.Location);

        final DSRequest request = new DSRequest();


        final DSResponse response = handler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
                 {
                     status: 0,
                     startRow: 0,
                     endRow: 6,
                     totalRows: 6,
                     data:[
                         {
                             id:1,
                             name: 'admin',
                             location: 1,
                             locationCity: 'Kharkiv'
                         },
                         {
                             id:2,
                             name: 'developer',
                             location: 2,
                             locationCity: 'Lviv'
                         },
                         {
                             id:3,
                             name: 'UseR3',
                             location: 3,
                             locationCity: 'USA'
                         },
                         {
                             id:4,
                             name: 'manager1',
                             location: 1,
                             locationCity: 'Kharkiv'
                         },
                         {
                             id:5,
                             name: 'manager2',
                             location: 2,
                             locationCity: 'Lviv'
                         },
                         {
                             id:6,
                             name: 'user2',
                             location: 3,
                             locationCity: 'USA'
                         }
                     ]
                }""", response);
    }

    @Test
    public void fetchPaginatedWithSort() throws Exception {

        DSRequest request = new DSRequest();
        request.setStartRow(0);
        request.setEndRow(2);

        // -- check default order (should be ascending)
        request.setSortBy(Collections.singletonList("name"));
        final DSResponse response1 = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
            {
                status: 0,
                startRow: 0,
                endRow: 2,
                totalRows: 6,
                data:[
                    {
                        id:3,
                        name: 'UseR3'
                    },
                    {
                        id:1,
                        name: 'admin'
                    }
                ]    
            }""", response1);

        // -- check descending
        request.setSortBy(Collections.singletonList("-name"));

        final DSResponse response2 = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
            {
                status: 0,
                startRow: 0,
                endRow: 2,
                totalRows: 6,
                data:[
                    {
                        id:6,
                        name: 'user2'
                    },
                    {
                        id:5,
                        name: 'manager2'
                    }
                ]    
            }""", response2);

        // -- check ascending
        request.setSortBy(Collections.singletonList("+name"));
        final DSResponse response3 = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
            {
                status: 0,
                startRow: 0,
                endRow: 2,
                totalRows: 6,
                data:[
                    {
                        id:3,
                        name: 'UseR3'
                    },
                    {
                        id:1,
                        name: 'admin'
                    }
                ]    
            }""", response3);
    }

    @Test
    public void fetchWithTextFilter() throws Exception {
        withExtraFields(ExtraField.Email);

        DSRequest request = new DSRequest();
        request.setStartRow(0);
        request.setEndRow(2);
        request.setTextMatchStyle(DSRequest.TextMatchStyle.SUBSTRING);
        request.wrapAndSetData(Map.of("email", "pm2"));

        // -- check default order (should be ascending)
        final DSResponse response = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
            {
                status: 0,
                startRow: 0,
                endRow: 1,
                totalRows: 1,
                data:[
                    {
                        id:5,
                        name: 'manager2',
                        email: 'pm2@acme.org'
                    }
                ]    
            }""", response);
    }

    @Test
    public void fetchWithIncludeFromField() throws Exception {
        withExtraFields(ExtraField.IncludeFromLocation);
        withHandlers(Handler.Location);

        DSRequest request = new DSRequest();
        request.setStartRow(0);
        request.setEndRow(2);
        request.setTextMatchStyle(DSRequest.TextMatchStyle.SUBSTRING);
        request.wrapAndSetData(Map.of("location", 1));

        // -- check default order (should be ascending)
        final DSResponse response = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
            {
                status: 0,
                startRow: 0,
                endRow: 2,
                totalRows: 2,
                data:[
                    {
                        id: 1,
                        name: 'admin',
                        location: 1,
                        locationCity: 'Kharkiv'
                    },
                    {
                        id: 4,
                        name: 'manager1',
                        location: 1,
                        locationCity: 'Kharkiv'
                    }
                ]    
            }""", response);
    }

    @Test
    public void fetchWithSQLCalculatedField() throws Exception {
        withExtraFields(ExtraField.SqlCalculated);

        DSRequest request = new DSRequest();
        request.setStartRow(0);
        request.setEndRow(2);

        final DSResponse response = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals("""
            {
                status:0,
                startRow:0,
                endRow:2,
                totalRows:6,
                data: [
                    {
                        id:1,
                        name:"admin",
                        calculated:"1_admin"
                    },
                    {
                        id:2,
                        name:"developer",
                        calculated:"2_developer"
                    }
                ]
            }""",
                response);

    }

    @Regression("Fails to fetch SQL Calculated Field via foreignDisplayField for a PK ")
    @Test
    public void fetchWithSQLCalculatedAsDisplayFieldForForeignRelation() throws Exception {
        final JDBCHandler h = withHandlers(Handler.EmployeeRole);
        withExtraFields(ExtraField.SqlCalculated);

        final DSRequest request = new DSRequest();
        final DSResponse response = h.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
            {
              status:0,
              startRow:0,
              endRow:5,
              totalRows:5,
              data:[
                 {
                    role:'Admin',
                    employee:1,
                    employeeCalculated:'1_admin'
                 },
                 {
                    role:'Developer',
                    employee:1,
                    employeeCalculated:'1_admin'
                 },
                 {
                    role:'Developer',
                    employee:2,
                    employeeCalculated:'2_developer'
                 },
                 {
                    role:'PM',
                    employee:4,
                    employeeCalculated:'4_manager1'
                 },
                 {
                    role:'PM',
                    employee:5,
                    employeeCalculated:'5_manager2'
                 }
              ]
            }""", response);
    }

    /**
     * Check case when a source Data Source does not have a correspondent FK field[s] and values
     * are mapped by so called "mappedBy" JPA mechanism using the source PK to Join On the concrete records
     * in foreign Data Source by  foreign Data Source FKs to the source Data Source (backward direction).
     */
    @Test
    public void fetchOneToMany_EntireEntity() throws Exception {
        withHandlers(Handler.EmployeeRole);
        withExtraFields(ExtraField.OneToMany_FetchEntireEntities, ExtraField.SqlCalculated);

        final DSRequest request = new DSRequest();

        /*
         * Fetch with  AdditionalOutput should return requested fields and PKs.
         */
        request.setAdditionalOutputs(
                "roles!EmployeeRoleDS.role," +
                        "roles!EmployeeRoleDS.employee," +
                        "roles!EmployeeRoleDS.employeeCalculated");

        final DSResponse response = handler.handleFetch(request);


        JsonTestSupport.assertJsonEquals("""
                {
                  status:0,
                  startRow:0,
                  endRow:6,
                  totalRows:6,
                  data:[
                     {
                        id:1,
                        name:'admin',
                        calculated:'1_admin', 
                        roles:[
                           {
                              role:'Admin',
                              employee:1,
                              employeeCalculated:'1_admin'
                           },
                           {
                              role:'Developer',
                              employee:1,
                              employeeCalculated:'1_admin'
                           }
                        ]
                     },
                     {
                        id:2,
                        name:'developer',
                        calculated:'2_developer', 
                        roles:[
                           {
                              role:'Developer',
                              employee:2,
                              employeeCalculated:'2_developer'
                           }
                        ]
                     },
                     {
                        id:3,
                        name:'UseR3',
                        calculated:'3_UseR3', 
                        roles:[]
                     },
                     {
                        id:4,
                        name:'manager1',
                        calculated:'4_manager1', 
                        roles:[
                            {
                                role:'PM',
                                employee:4,
                                employeeCalculated:'4_manager1'
                             }
                        ]
                     },
                     {
                        id:5,
                        name:'manager2',
                        calculated:'5_manager2', 
                        roles:[
                            {
                                role:'PM',
                                employee:5,
                                employeeCalculated:'5_manager2'
                             }
                        ]
                     },
                     {
                        id:6,
                        name:'user2',
                        calculated:'6_user2', 
                        roles:[
                        ]
                     }                     
                  ]
                }""", response);
    }


    @Test
    public void fetchOneToMany_OnlyIds() throws Exception {
        withHandlers(Handler.EmployeeRole);
        withExtraFields(ExtraField.OneToMany_FetchOnlyIds, ExtraField.SqlCalculated);

        final DSRequest request = new DSRequest();
        request.setOutputs("id, name, roles");
        final DSResponse response = handler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
            {
              status:0,
              startRow:0,
              endRow:6,
              totalRows:6,
              data:[
                 {
                    id:1,
                    name:'admin',
                    roles:[
                       {
                          role:'Admin',
                          employee:1
                       },
                       {
                          role:'Developer',
                          employee:1
                       }
                    ]
                 },
                 {
                    id:2,
                    name:'developer',
                    roles:[
                       {
                          role:'Developer',
                          employee:2
                       }
                    ]
                 },
                 {
                    id:3,
                    name:'UseR3',
                    roles:[
        
                    ]
                 },
                 {
                    id:4,
                    name:'manager1',
                    roles:[
                       {
                          role:'PM',
                          employee:4
                       }
                    ]
                 },
                 {
                    id:5,
                    name:'manager2',
                    roles:[
                       {
                          role:'PM',
                          employee:5
                       }
                    ]
                 },
                 {
                    id:6,
                    name:'user2',
                    roles:[
                    ]
                 }
              ]
            }""", response);
    }

    @Regression("Fails to fetch with order by SQL Calculated Field")
    @Test
    public void fetchWithOrderBySQLCalculatedField() throws Exception {
        withExtraFields(ExtraField.SqlCalculated);

        final DSRequest request = new DSRequest();
        request.setOutputs("id, calculated");
        request.setSortBy(List.of("calculated"));

        final DSResponse response = handler.handleFetch(request);
        JsonTestSupport.assertJsonEquals_WithOrder("""
            {
              status:0,
              startRow:0,
              endRow:6,
              totalRows:6,
              data:[
                 {
                    id:1,
                    calculated:'1_admin' 
                 },
                 {
                    id:2,
                    calculated:'2_developer' 
                 },
                 {
                    id:3,
                    calculated:'3_UseR3' 
                 },
                 {
                    id:4,
                    calculated:'4_manager1' 
                 },
                 {
                    id:5,
                    calculated:'5_manager2' 
                 },                  
                 {
                    id:6,
                    calculated:'6_user2' 
                 }                  
              ]
            }""", response);
    }

    @Test
    @Regression("Requests with multiple 'includeFrom' fails due to improper JOIN clause generation")
    public void fetchWithMultipleJoin() throws Exception {
        final JDBCHandler projectHandler =  withHandlers(Handler.Client, Handler.Project);
        withExtraFields(ExtraField.SqlCalculated);
        withExtraFields(projectHandler, ExtraField.Project_IncludeFromClient, ExtraField.Project_IncludeManagerFromEmployee);

        DSRequest request = new DSRequest();
        final DSResponse response = projectHandler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
            {
               status:0,
               startRow:0,
               endRow:5,
               totalRows:5,
               data:[
                  {
                     id:1,
                     name:'Project 1 for client 1',
                     client:1,
                     clientName:'client 1',
                     manager:4,
                     employeeFullName:'4_manager1'
                  },
                  {
                     id:2,
                     name:'Project 2 for client 1',
                     client:1,
                     clientName:'client 1',
                     manager:4,
                     employeeFullName:'4_manager1'
                  },
                  {
                     id:3,
                     name:'Project 1 for client 2',
                     client:2,
                     clientName:'client 2',
                     manager:5,
                     employeeFullName:'5_manager2'
                  },
                  {
                     id:4,
                     name:'Project 2 for client 2',
                     client:2,
                     clientName:'client 2',
                     manager:5,
                     employeeFullName:'5_manager2'
                  },
                  {
                     id:5,
                     name:'Project 3 for client 2',
                     client:2,
                     clientName:'client 2',
                     manager:5,
                     employeeFullName:'5_manager2'
                  }
               ]                
            }""", response);
    }

    @Test
    @Regression("Requests with multiple 'includeFrom' for the same foreign data source fails due to improper JOIN clause generation" +
            " duplicate join is generated for the same table.")
    public void fetchWithMultipleIncludeFromToTheSameJoin() throws Exception {
        final JDBCHandler projectHandler =  withHandlers(Handler.Client, Handler.Project);

        withExtraFields(ExtraField.SqlCalculated, ExtraField.Email);

        withExtraFields(projectHandler,
                ExtraField.Project_IncludeManagerFromEmployee,
                ExtraField.Project_IncludeManagerEmailFromEmployee
        );

        DSRequest request = new DSRequest();
        final DSResponse response = projectHandler.handleFetch(request);

        JsonTestSupport.assertJsonEquals("""
            {
               status:0,
               startRow:0,
               endRow:5,
               totalRows:5,
               data:[
                  {
                    id:1,
                    name:'Project 1 for client 1',
                    manager:4,
                    employeeFullName:'4_manager1',
                    manager_email:'pm1@acmE.org'
                  },
                  {
                    id:2,
                    name:'Project 2 for client 1',
                    manager:4,
                    employeeFullName:'4_manager1',
                    manager_email:'pm1@acmE.org'
                  },
                  {
                    id:3,
                    name:'Project 1 for client 2',
                    manager:5,
                    employeeFullName:'5_manager2',
                    manager_email:'pm2@acme.org'
                  },
                  {
                    id:4,
                    name:'Project 2 for client 2',
                    manager:5,
                    employeeFullName:'5_manager2',
                    manager_email:'pm2@acme.org'
                  },
                  {
                    id:5,
                    name:'Project 3 for client 2',
                    manager:5,
                    employeeFullName:'5_manager2',
                    manager_email:'pm2@acme.org'
                  }               
               ]
            }""", response);
    }
}
