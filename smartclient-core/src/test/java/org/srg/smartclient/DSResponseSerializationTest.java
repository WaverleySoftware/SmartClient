package org.srg.smartclient;

import net.javacrumbs.jsonunit.JsonAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.srg.smartclient.isomorphic.DSField;
import org.srg.smartclient.isomorphic.DSResponse;
import org.srg.smartclient.isomorphic.DSResponseDataContainer;
import org.srg.smartclient.utils.Serde;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Check that response is serialized in the  format expected by SmartClient JS side
 */
public class DSResponseSerializationTest {

    final static DSResponse SIMPLE_RESPONSE = DSResponse.successFetch(0,1,
            Arrays.asList(
                    new DSField()
                            .setName("field1")
                            .setType(DSField.FieldType.INTEGER)
                            .setPrimaryKey(true)
                    ,
                    new DSField()
                            .setName("field2")
                            .setType(DSField.FieldType.TEXT)
            ),
            Arrays.asList(
                    new Object[]{24, "24"},
                    new Object[]{42, "42"}
            )
    );

    final static List<DSField> subEntityFields =
            Arrays.asList(
                    new DSField()
                            .setName("cityId")
                            .setType(DSField.FieldType.INTEGER)
                            .setPrimaryKey(true)
                    ,
                    new DSField()
                            .setName("cityName")
                            .setType(DSField.FieldType.TEXT)
            );

    final static DSResponse ENTITY_FIELD_RESPONSE = DSResponse.successFetch(0,2,
            Arrays.asList(
                    new DSField()
                            .setName("countryId")
                            .setType(DSField.FieldType.INTEGER)
                            .setPrimaryKey(true),
                    new DSField()
                            .setName("countryName")
                            .setType(DSField.FieldType.TEXT),
                    new DSField()
                            .setName("cities")
                            .setType(DSField.FieldType.ENTITY)
            ),
            Arrays.asList(
                    new Object[]{
                            1,
                            "Lithuania",
                            new DSResponseDataContainer.RawDataResponse(
                                    subEntityFields,
                                    Arrays.asList(
                                            new Object[]{1, "Vilnius"},
                                            new Object[]{4, "Kaunas"}
                                    )
                            )
                    },
                    new Object[]{2, "Latvia",
                            new DSResponseDataContainer.RawDataResponse(
                                    subEntityFields,
                                    Collections.singletonList(new Object[]{2, "Riga"})
                            )
                    }
            )
    );


    private static String serializeResponse(DSResponse response) throws IOException {
        final StringWriter sw = new StringWriter();
        Serde.serializeResponse(sw, null, List.of(response));
        return sw.toString();
    }

    @Test
    public void deserializeSuccessfulEmptyResponse() throws IOException {
        final DSResponse response = DSResponse.successFetch(0, 0, 0,
                Arrays.asList(
                        new DSField()
                                .setName("name")
                                .setType(DSField.FieldType.TEXT)
                                .setDbName("name"),
                        new DSField()
                                .setName("client")
                                .setRequired(true)
                                .setType(DSField.FieldType.INTEGER)
                                .setForeignKey("ClientDS.id")
                                .setDbName("client_id"),
                        new DSField()
                                .setName("id")
                                .setRequired(true)
                                .setPrimaryKey(true)
                                .setType(DSField.FieldType.INTEGER)
                                .setDbName("id")
                ),
                new LinkedList<>()
        );

        JsonAssert.assertJsonEquals("""
            {                
                response:{
                    status:0,
                    startRow:0,
                    endRow:0,
                    totalRows:0,
                    data:[]
                }
            }""", serializeResponse(response));
    }

    @Test
    public void deserializeSuccessfulResponse() throws IOException {

        JsonAssert.assertJsonEquals("""
            {
                "response":{
                   "status":0,
                   "startRow":0,
                   "endRow":1,
                   "totalRows":-1,
                   "data":[
                      {
                         "field1":24,
                         "field2":"24"
                      },
                      {
                         "field1":42,
                         "field2":"42"
                      }
                   ]
                }
            }""", serializeResponse(SIMPLE_RESPONSE));
    }

    @Test
    public void deserializeSuccessfulResponseWithEntityField() throws IOException {
        JsonAssert.assertJsonEquals("""
            {
                response:{
                    status:0,
                    startRow:0,
                    endRow:2,
                    totalRows:-1,
                    data:[
                        {
                            countryName:'Lithuania',
                            countryId:1,
                            cities:[
                                {
                                    cityId:1,
                                    cityName:'Vilnius'
                                },
                                {
                                    cityId:4,
                                    cityName:'Kaunas'
                                }
                            ]
                        },
                        {
                            countryName:'Latvia',
                            countryId:2,
                            cities:[
                                {
                                    cityId:2,
                                    cityName:'Riga'
                                }
                            ]
                        }
                    ]
                }
            }""", serializeResponse(ENTITY_FIELD_RESPONSE));
    }

    @Test
    public void serializeSimpleResponseAsCSV() throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (final OutputStreamWriter w = new OutputStreamWriter(bos, "UTF-8")){
            final DSResponseDataContainer.RawDataResponse rdr = ENTITY_FIELD_RESPONSE.getData().getRawDataResponse();
            final char sep = ',';

            Serde.serializeResponseAsCSV(w, sep, ENTITY_FIELD_RESPONSE, false);

            final String result = bos.toString();

            Assertions.assertEquals("""
                "countryId","countryName","cities"
                1,"Lithuania",[{"cityId":1,"cityName":"Vilnius"},{"cityId":4,"cityName":"Kaunas"}]
                2,"Latvia",[{"cityId":2,"cityName":"Riga"}]
                """, result);
        }
    }
}
