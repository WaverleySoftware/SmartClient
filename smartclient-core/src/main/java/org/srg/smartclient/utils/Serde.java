package org.srg.smartclient.utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.introspect.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.srg.smartclient.isomorphic.*;
import org.srg.smartclient.isomorphic.criteria.AdvancedCriteria;

import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.IdentifiableType;
import java.io.IOException;
import java.io.Writer;
import java.lang.annotation.IncompleteAnnotationException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Serde {

    public static <T extends IDSRequest> T deserializeRequest(String data) throws IOException {
        final ObjectMapper mapper = createMapper();

        try (JsonParser p = mapper.createParser(data)) {
            if (p.nextToken() == JsonToken.START_OBJECT
                && "transaction".equals(p.nextFieldName())
                    && p.nextToken() == JsonToken.START_OBJECT
            ) {
                return (T)mapper.readValue(p, DSTransaction.class);
            } else {
                return (T)mapper.readValue(data, DSRequest.class);
            }
        }
    }

    public static void serializeResponseAsCSV(Writer writer, char separator,  DSResponse response) throws IOException {
        final DSResponseDataContainer.RawDataResponse rdr = response.getData().getRawDataResponse();
        final ObjectMapper mapper = Serde.createMapper();

        // -- write header
        boolean first = true;
        for (DSField dsf: rdr.getFields()) {
            if (!first){
                writer.append(separator);
            }
            writer.append('"');
            writer.write(dsf.getName());
            writer.append('"');
            first = false;
        }
        writer.write("\n");

        // -- write data
        for (Object[] r :rdr.getData()){
            first = true;
            for (Object v: r) {
                if (!first){
                    writer.write(separator);
                }
                if (v != null) {
                    if (v instanceof String) {
                        writer.append('"');
                        writer.write((String) v);
                        writer.append('"');
                    } else if (v instanceof DSResponseDataContainer.RawDataResponse subValue) {
                        final String str = mapper.writeValueAsString(subValue);
                        writer.write(str);
                    } else {
                        writer.write(v.toString());
                    }
                } else {
                    writer.write("");
                }

                first = false;
            }
            writer.write("\n");
        }

        writer.flush();
    }

    public static void serializeResponse(Writer writer, Integer transactionNum, Collection<DSResponse> responses) throws IOException {

        final boolean isQueue = responses.size() >1;

        final ObjectWriter objectWriter = createMapper()
                .writerWithDefaultPrettyPrinter()
                .withRootName("response");

        int queueStatus = 0;
        if (isQueue) {
            writer.append("[");

            for (DSResponse r : responses) {
                if (r.getStatus() != 0) {
                    queueStatus = -1;
                    break;
                }
            }
        }

        boolean isFirst = true;

        for (DSResponse r:responses) {
            if (!isFirst) {
                writer.append(",\n");
            } else {
                isFirst = false;
            }

            if (isQueue) {
                r.setQueueStatus(queueStatus);
                r.setTransactionNum(transactionNum);
            }

            objectWriter.writeValue(writer, r);
        }

        if (isQueue) {
            writer.append("]");
        }
    }

    public static String toJson(Object o) throws JsonProcessingException {
        final ObjectMapper om = createMapper();

        return om.writeValueAsString(o);
    }

    private static class DSRequestDeserializer extends JsonDeserializer<IDSRequestData> {
        @Override
        public IDSRequestData deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            final ObjectNode node = p.readValueAsTree();

            if (node.has("operator")) {
                return  p.getCodec().treeToValue(node, AdvancedCriteria.class);
            } else {
                return p.getCodec().treeToValue(node, DSRequest.MapData.class);
            }
        }
    }

    /**
     * This specific deserializer is required due to the requirement for the foreignDisplayField
     * mechanism that is described here:
     *   - https://www.smartclient.com/smartgwt/javadoc/com/smartgwt/client/docs/JpaHibernateRelations.html (One-to-Many Relations);
     *   - https://www.smartclient.com/smartclient-release/isomorphic/system/reference/?id=attr..DataSourceField.foreignDisplayField.
     *
     * Issue solved by this deserializer: In some use cases (subsequent entity fetch) <code>type</code> represents
     * a Data Source Id, whereas, usually, -- it represents a concrete field type,
     * one of the values from the enumeration {@link DSField.FieldType}. Therefore in thisrare case Jackson fails
     * to map <code>type</code>, since there are no correspondent enumeration value.
     *
     * https://stackoverflow.com/questions/18313323/how-do-i-call-the-default-deserializer-from-a-custom-deserializer-in-jackson
     */
    private static class DSFieldDeserializer extends StdDeserializer<DSField> {

        protected DSFieldDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public DSField deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            final ObjectNode node = p.readValueAsTree();

            if (node.has("type")) {
                final TextNode nodeV = (TextNode) node.get("type");
                final String typeV = nodeV.asText(null);

                try {
                    final DSField.FieldType ft = DSField.FieldType.valueOf(typeV.toUpperCase());
                    assert ft != null;
                }  catch (Throwable e) {
                    /**
                     *  Since field type doe not represent particular DSField.FieldType: it must have been
                     *  a Data Source ID, and the right field type for a suuch field is DSField.FieldType.Entity.
                     */
                    node.set("type", new TextNode("entity"));
                }
            }

            final DeserializationConfig config = ctxt.getConfig();
            JavaType type = TypeFactory.defaultInstance().constructType(DSField.class);
            JsonDeserializer<Object> defaultDeserializer = BeanDeserializerFactory.instance.buildBeanDeserializer(ctxt, type, config.introspect(type));

            if (defaultDeserializer instanceof ResolvableDeserializer) {
                ((ResolvableDeserializer) defaultDeserializer).resolve(ctxt);
            }

            final ObjectCodec oc = p.getCodec();
            JsonParser treeParser = oc.treeAsTokens(node);
            config.initialize(treeParser);

            if (treeParser.getCurrentToken() == null) {
                treeParser.nextToken();
            }

            return (DSField) defaultDeserializer.deserialize(treeParser, ctxt);
        }
    }

    private static class DSResponseDataContainerSerializer extends JsonSerializer<DSResponseDataContainer> {

        @Override
        public void serialize(DSResponseDataContainer rc, JsonGenerator jg, SerializerProvider serializers) throws IOException {
            switch (rc.getResponseType()) {
                case GENERAL_ERROR -> jg.writeString(rc.getGeneralFailureMessage());
                case RAW -> {
                    final DSResponseDataContainer.RawDataResponse rr = rc.getRawDataResponse();
                    jg.writeObject(rr);
                }
            }
        }
    }

    public static class RawDataResponseSerializer extends JsonSerializer<DSResponseDataContainer.RawDataResponse> {

        public static String SERIALIZE_FIELDS_ONLY = "SERIALIZE_FIELDS_ONLY";

        protected void serializeFieldsOnly(DSResponseDataContainer.RawDataResponse rr, JsonGenerator jg, SerializerProvider serializers) throws IOException {

            final List<String> names = StreamSupport.stream(rr.getFields().spliterator(), false)
                    .map(DSField::getName)
                    .collect(Collectors.toList());

            jg.writeStartObject();
            jg.writeObjectField("resulting-fields", names);
            jg.writeEndObject();
        }

        @Override
        public void serialize(DSResponseDataContainer.RawDataResponse rr, JsonGenerator jg, SerializerProvider serializers) throws IOException {
            final boolean isFieldsOnly = serializers.getAttribute(RawDataResponseSerializer.SERIALIZE_FIELDS_ONLY) != null;

            if (isFieldsOnly) {
                // Reduce verbosity for logging
                serializeFieldsOnly(rr, jg, serializers);
                return;
            }

            jg.writeStartArray();

            final LinkedHashMap<String, Object> data = new LinkedHashMap<>();
            final LinkedList<DSField> fields = new LinkedList<>();

            rr.getFields().forEach( dsf -> {
                data.put(dsf.getName(), null);
                fields.add(dsf);
            });

            for (Object r[]: rr.getData()) {
                int[] idx= {0};
                data.replaceAll((k,v) -> {
                    int i = idx[0];
                    final DSField f = fields.get(i);
                    final Object formattedValue = formatValue(f, r[i]);
                    idx[0]++;
                    return formattedValue;
                });
                jg.writeObject(data);
            }
            jg.writeEndArray();
        }

        private static Object formatValue(DSField field, Object value) {
            if (/*field.getIncludeFrom() != null && !field.getIncludeFrom().isBlank() &&*/ field.getType() ==  null) {
                /*
                 * field type can be null for nested entities and should be treated as
                 * {@code DSField.FieldType.ENTITY}.
                 *
                 * This may happen in case when the only Ids should be fetched for a sub-entity.
                 */
                return value;
            }

            switch (field.getType()) {
                case TEXT:
                    /**
                     * For PostgreSQL in case of jsonb, value will be a PGobject instance, therefore for proper
                     * serialization it is required to use value.toString() method.
                     *
                     * Otherwise, it will be serialized improperly:
                     *
                     * "changedData" : {
                     *      "type" : "jsonb",
                     *      "value" : "{\"id\": 12, \"name\": \"Zmags Publicator\", \"zoho_id\": 401759000008069015, \"client_id\": 3, \"created_at\": null, \"manager_id\": 41, \"modified_at\": \"2020-07-28T19:43:46.384\"}"
                     * }
                     */
                    if (value != null) {
                      return value.toString();
                    } else {
                        return  null;
                    }
                    //jsonGenerator.writeString((String) value);
                    //break;

                case INTEGER:
//                    jsonGenerator.writeNumber((Long)value);
//                    break;

                case DATETIME:

                case INTENUM:

                // TODO: write ENUM name instead of  writing ordinal
                case ENUM:
//                    jsonGenerator.writeString(value.toString());
//                    jsonGenerator.writeObject(value);
                case FLOAT:
                case BOOLEAN:
                    return value;

                case DATE:
                    if (value != null) {
                        return "%tF".formatted(value);
                    } else {
                        return null;
                    }

                case TIME:
                    if (value != null) {
                        return "%tT".formatted(value);
                    } else {
                        return null;
                    }

                case ENTITY:
                    return value;

                default:
                    throw new IllegalStateException("Unsupported DSField type '%s'.".formatted(field.getType()));
            }
        }
    }

//    private static class DSResponseSerialize extends  JsonSerializer<DSResponse> {
//        @Override
//        public void serialize(DSResponse dsResponse, JsonGenerator jg, SerializerProvider serializerProvider) throws IOException {
////            JavaType javaType = serializerProvider.constructType(DSResponse.class);
////            BeanDescription beanDesc = serializerProvider.getConfig().introspect(javaType);
////            JsonSerializer<Object> serializer = BeanSerializerFactory.instance.findBeanSerializer(serializerProvider,
////                    javaType,
////                    beanDesc);
////
////
////            BeanSerializer beanSerializer = (BeanSerializer) serializer;
////            beanSerializer.resolve(serializerProvider);
//            jg.writeStartObject();
//            jg.writeFieldName("response");
//            jg.writeStartObject();
//            jg.writeNumberField("status", dsResponse.getStatus());
//            jg.writeNumberField("startRow", dsResponse.getStartRow());
//            jg.writeNumberField("endRow", dsResponse.getEndRow());
//            jg.writeNumberField("totalRows", dsResponse.getTotalRows());
//
//            final DSResponseDataContainer rc = dsResponse.getData();
//            switch (dsResponse.getData().getResponseType()) {
//                case GENERAL_ERROR:
//                    jg.writeStringField("data", rc.getGeneralFailureMessage());
//                    break;
//
//                case RAW:
//                    jg.writeFieldName("data");
//                    jg.writeObject(rc.getRawDataResponse());
//                    break;
//
//            }
//            jg.writeEndObject();
//            jg.writeEndObject();
//        }
//    }

    private static class IdentifiableTypeSerializer extends JsonSerializer<IdentifiableType> {
        @Override
        public void serialize(IdentifiableType value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            //value.sourceType().getJavaType().getSimpleName()
            gen.writeString(value.getJavaType().getSimpleName());
        }
    }

    private static class AttributeSerializer extends JsonSerializer<Attribute> {
        @Override
        public void serialize(Attribute value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            //gen.writeStringField("sourceAttribute", value.sourceAttribute().getName());
            gen.writeString(value.getName());
        }
    }

//    private static class JpaRelationSerializer extends JsonSerializer<JpaRelation> {
//
//        @Override
//        public void serialize(JpaRelation value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
//            gen.writeStartObject();
//
//            gen.writeStringField("type", value.type().name());
//            gen.writeStringField("sourceType", value.sourceType().getJavaType().getSimpleName());
//            gen.writeStringField("sourceAttribute", value.sourceAttribute().getName());
//
//            gen.writeStringField("targetType", value.targetType().getJavaType().getSimpleName());
//
//            if (value.targetAttribute() != null) {
//                gen.writeStringField("targetAttribute", value.targetAttribute().getName());
//            } else {
//                gen.writeNullField("targetAttribute");
//            }
//
//            gen.writeBooleanField("isInverse", value.isInverse());
//
//            gen.writeObjectField("joinColumns", value.joinColumns());
//            gen.writeObjectField("joinTable", value.joinTable());
//
//            gen.writeEndObject();
//        }
//    }

    private static class JoinColumnSerializer extends JsonSerializer<JoinColumn> {
        @Override
        public void serialize(JoinColumn value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();

            String name;
            try {
                name = value.name();
            } catch (IncompleteAnnotationException e) {
                name = "";
            }
            gen.writeStringField("name", name);

            boolean nullable;
            try {
                nullable = value.nullable();
            } catch (IncompleteAnnotationException e){
                nullable = true;
            }
            gen.writeBooleanField("nullable", nullable);

            boolean insertable;
            try {
                insertable = value.insertable();
            } catch (IncompleteAnnotationException e){
                insertable = true;
            }
            gen.writeBooleanField("insertable", insertable);

            boolean unique;
            try {
                unique = value.unique();
            } catch (IncompleteAnnotationException e){
                unique = false;
            }
            gen.writeBooleanField("unique", unique);

            boolean updatable;
            try {
                updatable = value.updatable();
            } catch (IncompleteAnnotationException e){
                updatable = true;
            }
            gen.writeBooleanField("updatable", updatable);

//            gen.writeObjectField("foreignKey", value.foreignKey());
//            gen.writeStringField("columnDefinition", value.columnDefinition());

            String referencedColumnName;
            try {
                referencedColumnName = value.referencedColumnName();
            } catch (IncompleteAnnotationException e){
                referencedColumnName = "";
            }
            gen.writeStringField("referencedColumnName", referencedColumnName);

            String table;
            try {
                table = value.table();
            } catch (IncompleteAnnotationException e){
                table = "";
            }
            gen.writeStringField("table", table);


            gen.writeEndObject();
        }
    }

    private static class JoinTableSerializer extends JsonSerializer<JoinTable> {
        @Override
        public void serialize(JoinTable value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();

            String name;
            try {
                name = value.name();
            } catch (IncompleteAnnotationException e) {
                name = "";
            }
            gen.writeStringField("name", name);

            JoinColumn[] joinColumns;
            try {
                joinColumns = value.joinColumns();
            } catch (IncompleteAnnotationException e) {
                joinColumns = null;
            }
            gen.writeObjectField("joinColumns", joinColumns);

            JoinColumn[] inverseJoinColumns;
            try {
                inverseJoinColumns = value.inverseJoinColumns();
            } catch (IncompleteAnnotationException e) {
                inverseJoinColumns = null;
            }
            gen.writeObjectField("inverseJoinColumns", inverseJoinColumns);


            gen.writeEndObject();
        }
    }

    // The following  AnnotationIntrospector is required to fix issues with Record deserialization,
    // that is a technical preview and does not supported by Jackson from out of the box.
    public static JacksonAnnotationIntrospector createAnnotationIntrospector() {
        return new JacksonAnnotationIntrospector() {
            @Override
            public PropertyName findNameForDeserialization(Annotated a) {
                PropertyName nameForDeserialization = super.findNameForDeserialization(a);

                if (a instanceof AnnotatedMember
                        && ((AnnotatedMember) a).getDeclaringClass().isRecord()) {

                    final JsonIgnoreProperties.Value v = super.findPropertyIgnorals(a);
                    // Even when @JsonProperty  is not used that's no reason to ignore it
                    // Until it is not ignored explicditly
                    if (nameForDeserialization == null

                            // when @JsonDeserialize is used, USE_DEFAULT is default
                            // preventing the implicit constructor to be found
                            || PropertyName.USE_DEFAULT.equals(nameForDeserialization)) {
                        String str = findImplicitPropertyName((AnnotatedMember) a);
                        if (str != null && !str.isEmpty()) {
                            return PropertyName.construct(str);
                        }
                    }

                }
                return nameForDeserialization;
            }

            @Override
            public String findImplicitPropertyName(AnnotatedMember m) {
                if (m.getDeclaringClass().isRecord()
                        && m instanceof AnnotatedParameter parameter) {
                    final String name = m.getDeclaringClass().getRecordComponents()[parameter.getIndex()].getName();
                    return name;
                } /*else if (m.getDeclaringClass().isRecord()
                        && m instanceof AnnotatedField af) {
                    return af.getName();
                }*/
                return super.findImplicitPropertyName(m);
            }
        };
    }


    public static ObjectMapper createMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        final SimpleModule module = new SimpleModule("DSResponse-Serialization", Version.unknownVersion());
//        module.addSerializer(DSResponse.class, new DSResponseSerialize() );
        module.addSerializer(DSResponseDataContainer.class, new DSResponseDataContainerSerializer());
        module.addSerializer(DSResponseDataContainer.RawDataResponse.class, new RawDataResponseSerializer());
//        module.addSerializer(JpaRelation.class, new JpaRelationSerializer());
        module.addSerializer(IdentifiableType.class, new IdentifiableTypeSerializer());
        module.addSerializer(Attribute.class, new AttributeSerializer());
        module.addSerializer(JoinColumn.class, new JoinColumnSerializer());
        module.addSerializer(JoinTable.class, new JoinTableSerializer());

        module.addDeserializer(IDSRequestData.class, new DSRequestDeserializer());
        module.addDeserializer(DSField.class, new DSFieldDeserializer(DSField.class));

        mapper.registerModule(module);

        mapper
                .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
//                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
//                .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
//                .enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
//                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature())
                .setAnnotationIntrospector(
                    createAnnotationIntrospector()
                );

        return mapper;
    }
}
