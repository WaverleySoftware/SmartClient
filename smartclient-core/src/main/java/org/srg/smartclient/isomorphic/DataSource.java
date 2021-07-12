package org.srg.smartclient.isomorphic;

import java.util.*;
import java.util.stream.Collectors;

// https://www.smartclient.com/smartgwt/javadoc/com/smartgwt/client/docs/serverds/DataSource.html
//https://www.smartclient.com/isomorphic/system/reference/?id=class..DataSource
public class DataSource {
    // https://www.smartclient.com/smartgwt/javadoc/com/smartgwt/client/types/DSServerType.html
    public enum DSServerType {
        GENERIC,
        HIBERNATE,
        JPA,
        JPA1,
        PROJECTFILE,
        SQL,
        JSON
    }

    public enum DeletionType {
        DELETE,
        SOFT_DELETE,
        NOT_AVAILABLE
    }

    private String id;
    private DSServerType serverType;
    private String dbName;
    private String beanClassName;
    private ServerObject serverObject;

    private String fileName;
    private List<DSField> fields;

    private transient Map<String, DSField> fieldMap;

    private String tableName;
    private DeletionType deletionType = DeletionType.NOT_AVAILABLE;

//    /**
//     * https://www.smartclient.com/smartclient-release/isomorphic/system/reference/?id=attr..DataSource.schemaBean
//     *
//     * For DataSources that specify autoDeriveSchema, this property indicates the name of the Spring bean,
//     * Hibernate mapping or fully-qualified Java class to use as parent schema.
//     *
//     */
//    private String schemaBean;
//
//    /**
//     * https://www.smartclient.com/smartclient-release/isomorphic/system/reference/?id=attr..DataSource.autoDeriveSchema
//     *
//     * This property allows you to specify that your DataSource's schema (field definitions)
//     * should be automatically derived from some kind of metadata.
//     */
//    private boolean autoDeriveSchema;

    private List<OperationBinding> operationBindings;

    private String serverConstructor;

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public DSServerType getServerType() {
        return serverType;
    }

    public void setServerType(DSServerType serverType) {
        this.serverType = serverType;
    }

    public String getBeanClassName() {
        return beanClassName;
    }

    public void setBeanClassName(String beanClassName) {
        this.beanClassName = beanClassName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public List<DSField> getFields() {
        return fields;
    }

    public DSField getField(String fieldName) {
        return getFieldMap().get(fieldName);
    }

    public void setFields(List<DSField> fields) {
        fieldMap = null;

        /**
         * It is highly imported:
         *      PKs fields MUST BE teh first ones in the field list
         */
        final List<DSField> pksList = new ArrayList<>(fields.size());
        final List<DSField> othersList = new ArrayList<>(fields.size());

        for (DSField dsf: fields) {
            if (dsf.isPrimaryKey()) {
                pksList.add(dsf);
            } else {
                othersList.add(dsf);
            }
        }

        pksList.addAll(othersList);
        this.fields = Collections.unmodifiableList(pksList);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public DeletionType getDeletionType() {
        return deletionType;
    }

    public void setDeletionType(DeletionType deletionType) {
        this.deletionType = deletionType;
    }

    //    public String getSchemaBean() {
//        return schemaBean;
//    }
//
//    public void setSchemaBean(String schemaBean) {
//        this.schemaBean = schemaBean;
//    }
//
//    public boolean isAutoDeriveSchema() {
//        return autoDeriveSchema;
//    }
//
//    public void setAutoDeriveSchema(boolean autoDeriveSchema) {
//        this.autoDeriveSchema = autoDeriveSchema;
//    }

    public List<OperationBinding> getOperationBindings() {
        return operationBindings;
    }

    public void setOperationBindings(List<OperationBinding> operationBindings) {
        final List<OperationBinding> b = new ArrayList<>(operationBindings);
        this.operationBindings = Collections.unmodifiableList(b);
    }

    public String getServerConstructor() {
        return serverConstructor;
    }

    public void setServerConstructor(String serverConstructor) {
        this.serverConstructor = serverConstructor;
    }

    protected Map<String, DSField> getFieldMap() {
        if (fieldMap == null) {
            final Map<String, DSField> m = new LinkedHashMap<>();

            for (DSField f: this.getFields()) {
                m.put(f.getName(), f);
            }

            fieldMap = Collections.unmodifiableMap(m);
        }

        return fieldMap;
    }

    public Set<DSField> getPKFields() {
        return getFields().stream()
                .filter(DSField::isPrimaryKey)
                .collect(Collectors.toSet());
    }

    public DSField getNonCompositePK() {
        final Set<DSField> pks = getPKFields();

        switch (pks.size()) {
            case 1:
                return pks.iterator().next();

            case 0:
                throw new IllegalStateException(
                        "DataSource '%s': Does not have any PK."
                                .formatted(this.getId())
                );

            default:
                throw new IllegalStateException(
                        "DataSource '%s': Composite PKs are not supported."
                                .formatted(this.getId())
                );
        }
    }

    public ServerObject getServerObject() {
        return serverObject;
    }

    public void setServerObject(ServerObject serverObject) {
        this.serverObject = serverObject;
    }

    @Override
    public String toString() {
        return "DataSource{" +
                "id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataSource)) return false;
        if (!(o instanceof DataSource that)) return false;
        return getId().equals(that.getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
