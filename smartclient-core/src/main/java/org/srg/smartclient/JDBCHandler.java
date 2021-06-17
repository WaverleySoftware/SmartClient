package org.srg.smartclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.srg.smartclient.isomorphic.*;
import org.srg.smartclient.isomorphic.criteria.AdvancedCriteria;
import org.srg.smartclient.isomorphic.criteria.Criteria;
import org.srg.smartclient.isomorphic.criteria.OperatorId;
import org.srg.smartclient.utils.ContextualRuntimeException;
import org.srg.smartclient.utils.Utils;

import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.srg.smartclient.isomorphic.DSField.FieldType.TEXT;


public class JDBCHandler extends AbstractDSHandler {
    public interface JDBCPolicy {
        // http://java.avdiel.com/Tutorials/JDBCPaging.html
        void withConnectionDo(String database, Utils.CheckedFunction<Connection, Void> callback) throws Exception;
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final JDBCPolicy policy;

    public JDBCHandler(JDBCPolicy jdbcPolicy, IDSRegistry dsRegistry, DataSource datasource) {
        super(dsRegistry, datasource);
        policy = jdbcPolicy;
    }

    @Override
    protected DSResponse handleUpdate(DSRequest request) throws Exception {
        if (! (request.getData() instanceof Map)) {
            throw new RuntimeException("Bad request: operation 'UPDATE', the map of modified and PK fields " +
                    "must be provided in the  'data' field.");
        }

        final OperationBinding operationBinding = getEffectiveOperationBinding(DSRequest.OperationType.FETCH, request.getOperationId());
        final SQLUpdateContext<JDBCHandler> sqlUpdateContext = new SQLUpdateContext<>(this, request, operationBinding);


        // --
        final DSResponse[] response = {null};

        policy.withConnectionDo(this.getDataSource().getDbName(), conn-> {

            try (PreparedStatement st = conn.prepareStatement(sqlUpdateContext.getUpdateSQL(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                int idx =0;

                final List<IFilterData> l = sqlUpdateContext.getModifiedData();
                for (IFilterData fd: l) {
                    idx = fd.setStatementParameters(idx, st);
                }

                final List<IFilterData> ll = sqlUpdateContext.getPkFieldData();
                for (IFilterData fd: ll) {
                    idx = fd.setStatementParameters(idx, st);
                }

                final int qnt = st.executeUpdate();


                if (qnt == 0) {
                    // There is no updated/affected records
                    throw new RuntimeException("Zero rows were updated.");
                }
            } catch (Throwable t) {
                conn.rollback();
                throw new ContextualRuntimeException("SQL update query execution failed.", t, sqlUpdateContext);
            }

            /*
             * It is required to return modified row back to the client.
             *
             * This can be done either by:
             *  1) comprehensive fetch from DB
             *  2) by modifying "old values" sent from client w/o real  retch.
             *
             * The first option is more durable, since it will handle all side effects like calculated fields? operation bindings
             * with respect of security rules tat can be affected by side effects.
             */


            final DSRequest fr = new DSRequest();
            fr.setDataSource(request.getDataSource());
            fr.setOperationType(DSRequest.OperationType.FETCH);
            fr.setOperationId(request.getOperationId());
            fr.setComponentId(request.getComponentId());
            fr.wrapAndSetData(sqlUpdateContext.getPkValues());

            // return the only fields that were provided within the request, except metadata
            fr.setOutputs(
                request.getOldValues().keySet().stream()
                    .filter( s -> !s.startsWith(getMetaDataPrefix()))
                    .collect(Collectors.joining(", "))
            );

            final DSResponse r =  doHandleFetch(fr, conn, false);

            if (r.getStatus() == DSResponse.STATUS_SUCCESS ) {
                conn.commit();
            } else {
                conn.rollback();
            }

            response[0] = r;
            return null;
        });

        final DSResponse fetchRespone = response[0];

        if (fetchRespone.getStatus() != DSResponse.STATUS_SUCCESS) {
            return fetchRespone;
        }

        return DSResponse.success(fetchRespone.getData());
    }

    @Override
    protected DSResponse handleFetch(DSRequest request) throws Exception {
        final DSResponse[] r = {null};
        if (request instanceof StickyDBDSRequest sdbRequest) {
            r[0] = doHandleFetch(request, sdbRequest.connection(), true);
        } else {
            policy.withConnectionDo(this.getDataSource().getDbName(), conn -> {
                r[0] = doHandleFetch(request, conn, true);
                return null;
            });
        }

        return r[0];
    }

    @Override
    protected DSResponse handleAdd(DSRequest request) throws Exception {
        if (! (request.getData() instanceof Map)) {
            throw new RuntimeException("Bad request: operation 'ADD', the map of modified and PK fields " +
                    "must be provided in the  'data' field.");
        }

        final OperationBinding operationBinding = getEffectiveOperationBinding(DSRequest.OperationType.FETCH, request.getOperationId());
        final SQLAddContext<JDBCHandler> sqlAddContext = new SQLAddContext<>(this, request, operationBinding);


        // --
        final DSResponse[] response = {null};

        final LinkedList<DSRequest.MapData> pks = new LinkedList<>();

        policy.withConnectionDo(this.getDataSource().getDbName(), conn -> {

            try (PreparedStatement st = conn.prepareStatement(sqlAddContext.getAddSQL(),
                    Statement.RETURN_GENERATED_KEYS)) {

                final List<IFilterData> l = sqlAddContext.getFilterData();
                if (l != null && !l.isEmpty()) {
                    for (IFilterData fd : l) {
                        fd.setStatementParameters(0, st);
                    }
                }
                final int qnt = st.executeUpdate();


                if (qnt == 0) {
                    // There is no added/affected records
                    throw new RuntimeException("Zero rows were added.");
                }
                try (ResultSet generatedKeys = st.getGeneratedKeys()) {
                    while (generatedKeys.next()) {
                        ResultSetMetaData metaData = generatedKeys.getMetaData();
                        DSRequest.MapData map = new DSRequest.MapData();
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            String dbName = metaData.getColumnName(i);

                            Object key = generatedKeys.getObject(i);
                            this.getDataSource().getFields().stream()
                                    .filter(it -> it.getDbName().equals(dbName))
                                    .findFirst()
                                    .ifPresent(dsField -> { map.put(dsField.getName(), key); });
                        }
                        if (!map.isEmpty()) {
                            pks.add(map);
                        }
                    }
                }
            } catch (Throwable t) {
                conn.rollback();
                throw new ContextualRuntimeException("SQL add query execution failed.", t, sqlAddContext);
            }

            /*
             * It is required to return modified row back to the client.
             *
             * This can be done either by comprehensive fetch from DB
             *
             * The first option is more durable, since it will handle all side effects like calculated fields? operation bindings
             * with respect of security rules tat can be affected by side effects.
             */


            final DSRequest fr = new DSRequest();
            fr.setDataSource(request.getDataSource());
            fr.setOperationType(DSRequest.OperationType.FETCH);
            fr.setOperationId(request.getOperationId());
            fr.setComponentId(request.getComponentId());
            fr.setTextMatchStyle(DSRequest.TextMatchStyle.EXACT);

            List<DSResponse> responses = new LinkedList<>();
            for (DSRequest.MapData data : pks) {
                fr.setData(data);
                final DSResponse r = doHandleFetch(fr, conn, false);
                if (r.getStatus() == DSResponse.STATUS_SUCCESS) {
                    responses.add(r);
                } else {
                    conn.rollback();
                    return null;
                }

            }

            if (responses.isEmpty()) {
                conn.rollback();
                return null;
            }

            conn.commit();

            Iterable<DSField> fields = responses.get(0).getData().getRawDataResponse().getFields();
            List<Object[]> data = new LinkedList<>();
            responses.forEach(it ->
                    data.add(it.getData().getRawDataResponse().getData().iterator().next()));
            response[0] = DSResponse.success(DSResponseDataContainer.createRaw(fields, data));
            return null;
        });

        final DSResponse fetchRespone = response[0];

        if (fetchRespone.getStatus() != DSResponse.STATUS_SUCCESS) {
            return fetchRespone;
        }

        return DSResponse.success(fetchRespone.getData());
    }

    private static class StickyDBDSRequest extends DSRequest {
        private final Connection connection;

        public StickyDBDSRequest(Connection connection) {
            this.connection = connection;
        }

        public Connection connection() {
            return connection;
        }
    }

    private static class EntitySubFetch {
        private static final Logger logger = LoggerFactory.getLogger(EntitySubFetch.class);

        private final Map<String, Object> primaryKeys;
        private final DSField dsf;
        private final ForeignKeyRelation foreignKeyRelation;
        private final List<DSField> requestedFields;
        private final IDSLookup idsRegistry;
        private final boolean useSimpleCriteria;
        private final boolean fetchOnlyPKs;

        public EntitySubFetch(IDSLookup idsRegistry, DSField dsf, ForeignKeyRelation foreignKeyRelation,
                              List<DSField> requestedFields, Map<String, Object> primaryKeys,
                              boolean useSimpleCriteria) {
            this.idsRegistry = idsRegistry;
            this.primaryKeys = primaryKeys;
            this.dsf = dsf;
            this.foreignKeyRelation = foreignKeyRelation;

            if (requestedFields != null && !requestedFields.isEmpty()) {
                this.requestedFields = requestedFields;
                fetchOnlyPKs = false;
            } else {
                //this.requestedFields = foreignKeyRelation.foreign().dataSource().getFields();
                this.requestedFields = null;
                /*
                 * If fields are not requested - fetch only PKs
                 */
                fetchOnlyPKs = true;
            }

            this.useSimpleCriteria = useSimpleCriteria;
        }

        public Map<String, Object> getPrimaryKeys() {
            return primaryKeys;
        }

        public DSField getDsf() {
            return dsf;
        }

        public ForeignKeyRelation getForeignKeyRelation() {
            return foreignKeyRelation;
        }

        public List<DSField> getRequestedFields() {
            return requestedFields;
        }

        public static EntitySubFetch create(boolean useSimpleCriteria, IDSLookup idsRegistry,
                                            DataSource dataSource, DSField dsf, List<DSField> requestedFields,
                                            Map<String, Object> rowPkValues) {
            final ForeignKeyRelation foreignKeyRelation;

            if (dsf.getIncludeFrom() != null
                    && !dsf.getIncludeFrom().isBlank()) {
                /*
                 * Use 'includeFrom' if it is provided
                 */
                final ImportFromRelation importFromRelation = RelationSupport.describeImportFrom(idsRegistry, dataSource, dsf);
                foreignKeyRelation = importFromRelation.toForeignDisplayKeyRelation();
            } else {
                foreignKeyRelation = RelationSupport.describeForeignKey(idsRegistry, dataSource, dsf);
            }

            return new EntitySubFetch(idsRegistry, dsf, foreignKeyRelation, requestedFields,
                    rowPkValues, useSimpleCriteria);
        }

        protected DSResponse fetchForeignEntity(Connection connection, ForeignKeyRelation foreignKeyRelation, String outputs, IDSRequestData criteria) throws Exception {
            logger.debug("Performing foreign fetch for relation '%s' with criteria: %s"
                    .formatted(
                            foreignKeyRelation,
                            criteria
                    )
            );

            final DSHandler dsHandler = this.idsRegistry.getDataSourceHandlerById(foreignKeyRelation.foreign().dataSourceId());
            if (dsHandler == null) {
                throw new RuntimeException( "Foreign data source handler with id '%s' is not registered."
                        .formatted(foreignKeyRelation.foreign().dataSourceId())
                );
            }

            /*
             * Create sticky request to re-use the same DB connection
             */
            final DSRequest fetchEntity = new StickyDBDSRequest(connection);
            fetchEntity.setDataSource(dsHandler.id());
            fetchEntity.setOperationType(DSRequest.OperationType.FETCH);
            fetchEntity.setData(criteria);
            fetchEntity.setOutputs(outputs);

            /*
             * if type is not provided it indicates that the only PKs should be fetched.
             *
             * @see <a href="https://www.smartclient.com/smartgwt/javadoc/com/smartgwt/client/docs/JpaHibernateRelations.html">JPA & Hibernate Relations</a>
             */
            if (null == foreignKeyRelation.sourceField().getType()) {
                logger.trace(("Field type is not provided for the source field '%s.%s', therefore the only primary key[s] " +
                        "will be fetched by the foreign fetch %s.")
                        .formatted(
                                foreignKeyRelation.dataSource().getId(),
                                foreignKeyRelation.sourceField().getName(),
                                foreignKeyRelation
                        )
                );

                final DataSource foreignDS = foreignKeyRelation.foreign().dataSource();

                final String pkNames = foreignDS.getPKFields().stream()
                        .map(DSField::getName)
                        .collect(Collectors.joining(", "));

                fetchEntity.setOutputs(pkNames);
            }

            return dsHandler.handle(fetchEntity);
        }

        protected static Map<String, Object> retrieveIdsFromDb(Connection connection, DSField sourceField, ForeignKeyRelation foreignKeyRelation, Map<String, Object> pks) throws SQLException {

            if (pks.size() > 1) {
                throw new IllegalStateException("Composite PKs is not supported");
            }

            final DSField.JoinTableDescr jtd =sourceField.getJoinTable();
            final Set<Object> values = new HashSet<>();

            try (PreparedStatement st = connection.prepareStatement(
                    "SELECT %s FROM %s WHERE %s IN (?)"
                            .formatted(
                                    jtd.getDestColumn(),
                                    jtd.getTableName(),
                                    jtd.getSourceColumn()
                            ),
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {


                // -- Get Source PK value
                final DSField srcPk = foreignKeyRelation.dataSource().getNonCompositePK();
                final Object srcPkValue = pks.get(srcPk.getName());

                if (srcPkValue == null) {
                    // NOT sure what should be done in that case
                }

                // --
                st.setObject(1, srcPkValue);


                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        Object v = rs.getObject(1);
                        assert v != null;
                        values.add(v);
                    }
                }

                if (values.isEmpty()) {
                    /*
                     * There is no relevant records found, therefore,
                     * no needs to perform actual fetch
                     */
                    return null;
                }
            }

            // -- Create resulting Map<PkFieldName, PkValue>
            final DSField dstPk = foreignKeyRelation.foreign().dataSource().getNonCompositePK();
            return Map.of(dstPk.getName(), values);
        }

        public Object fetch(Connection connection) {

            final DSResponse response;
            try {
                final Map<String, Object> effectivePKs;

                if (getDsf().getJoinTable() != null) {
                    /*
                     * If join table is provided - it indicates that the relation is Many to Many.
                     * And to Handle Many To Many relation it is required to retrieve correspondent
                     * secondary ids from join table
                     */

                    effectivePKs = retrieveIdsFromDb(connection, getDsf(), foreignKeyRelation, getPrimaryKeys());
                } else {
                    final DSField pkField = foreignKeyRelation.dataSource().getNonCompositePK();
                    final Object v = getPrimaryKeys().get(pkField.getName());

                    if (v == null) {
                        // This will indicate serious error in the request handling logic, if any
                        throw new IllegalStateException("PK/FK value can not be null, but actually it is null: '%s.%s'."
                                        .formatted(
                                                foreignKeyRelation.dataSource().getId(),
                                                dsf.getName()
                                        )
                        );
                    }

                    effectivePKs = Map.of(foreignKeyRelation.foreign().fieldName(), v );
                }

                if (effectivePKs == null) {
                    /*
                     * It can happen when Many2Many does not have related records
                     */
                    return null;
                }

                boolean allKeysProvided = true;
                final Set<DSField> pkFields = getForeignKeyRelation().foreign().dataSource().getPKFields();
                for (DSField pkf: pkFields) {
                    if (!effectivePKs.containsKey(pkf.getName())) {
                        allKeysProvided = false;
                        break;
                    }
                }


                if (fetchOnlyPKs && allKeysProvided) {
                    /*
                     * No needs to fetch PKs
                     */

                    if (effectivePKs.size() > 1) {
                        throw new IllegalStateException("Composite PKs is not supported.");
                    }



                    final Map.Entry<String, Object> e = effectivePKs.entrySet().iterator().next();

                    // Not sure that it will work with composite PKs
                    assert pkFields.size() <= 1;

                    if (e.getValue() instanceof Collection c) {
                        final int size = c.size();
                        final List<Object[]> cc = (List)c.stream()
                                .map( v -> new Object[]{v} )
                                .collect(Collectors.toList());

                        response = DSResponse.successFetch(0, size-1, size, pkFields, cc);
                    } else {
                        response = DSResponse.successFetch(0, 1, 1, pkFields, (List)List.of(new Object[] {e.getValue()}));
                    }

                } else {

                    final Collection<DSField> effectiveFields = fetchOnlyPKs ? pkFields : getRequestedFields();
                    final String entityOutputs = effectiveFields.stream()
                                    .map(DSField::getName)
                                    .collect(Collectors.joining(", "));

                    // -- create Criteria to fetch by PK
                    if (primaryKeys.size() > 1) {
                        throw new IllegalStateException("Composite PKs is not supported");
                    }

                    final IDSRequestData filterData;

                    if (useSimpleCriteria) {
                        final DSRequest.MapData md = new DSRequest.MapData();
                        md.putAll(effectivePKs);
                        filterData = md;
                    } else {
                        final Map.Entry<String, Object> e = effectivePKs.entrySet().iterator().next();
                        final AdvancedCriteria ac = new AdvancedCriteria();
                        final Criteria c = new Criteria();

                        c.setOperator(OperatorId.IN_SET);
                        c.setValue(e.getValue());
                        c.setFieldName(e.getKey());

                        ac.setCriteria(List.of(c));
                        filterData = ac;
                    }

                    // --
                    response = this.fetchForeignEntity(connection, getForeignKeyRelation(), entityOutputs, filterData);
                }
                assert response != null;
            } catch ( Throwable t) {
                final String message = "Subsequent entity fetch failed: %s, filters: %s"
                        .formatted(
                                foreignKeyRelation,
                                getPrimaryKeys().entrySet().stream()
                                        .map( e -> "'%s': %s"
                                                .formatted(
                                                        e.getKey(),
                                                        e.getValue()
                                                )
                                        )
                                        .collect(Collectors.joining(","))
                        );

                logger.error(message, t);
                throw new RuntimeException(message, t);
            }

            if (0 != response.getStatus()) {
                throw new RuntimeException("Subsequent entity fetch failed: %s, %s, filters: %s"
                        .formatted(
                                response.getData().getGeneralFailureMessage(),
                                foreignKeyRelation,
                                getPrimaryKeys().entrySet().stream()
                                        .map( e -> "'%s': %s"
                                                .formatted(
                                                        e.getKey(),
                                                        e.getValue()
                                                )
                                        )
                                        .collect(Collectors.joining(","))
                        )
                );
            }

            Object o = response.getData().getRawDataResponse();
            assert o != null;

            return o;
        }
    }

    protected DSResponse doHandleFetch(DSRequest request, Connection connection, boolean calculateTotal) throws Exception {
        final OperationBinding operationBinding = getEffectiveOperationBinding(DSRequest.OperationType.FETCH, request.getOperationId());
        final SQLFetchContext<JDBCHandler> sqlFetchContext = new SQLFetchContext<>(this, request, operationBinding);

        final List<Object[]> data;
        if (sqlFetchContext.getPageSize() > 0) {
            data = new ArrayList<>(sqlFetchContext.getPageSize());
        } else {
            data = new LinkedList<>();
        }

        final int[] totalRows = new int[] {-1};

        // -- calculate total
        if (calculateTotal) {
            /*
             * Opaque query is required for a proper filtering by calculated fields
             */
            @SuppressWarnings("SqlNoDataSourceInspection") final String countQuery = "SELECT count(*) FROM %s"
                    .formatted(sqlFetchContext.getGenericQuery());

            if (logger.isTraceEnabled()) {
                logger.trace("DataSource %s fetch count(*) query:\n%s\n\nparams:\n%s"
                        .formatted(
                                getDataSource().getId(),
                                countQuery,
                                sqlFetchContext.getFilterData().stream()
                                        .flatMap(fd -> StreamSupport.stream(fd.values().spliterator(), false))
                                        .map("%s"::formatted)
                                        .collect(Collectors.joining(", "))
                        )
                );
            }

            sqlFetchContext.setEffectiveSQL(countQuery);

            try (PreparedStatement st = connection.prepareStatement(countQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                int idx = 0;

                for (IFilterData fd : sqlFetchContext.getFilterData()) {
                    idx = fd.setStatementParameters(idx, st);
                }

                try (ResultSet rs = st.executeQuery()) {
                    rs.next();
                    totalRows[0] = rs.getInt(1);
                }
            } catch (Throwable t) {
                throw new ContextualRuntimeException("SQL count query execution failed.", t, sqlFetchContext);
            }
        }

        // -- fetch data
        /*
         * Opaque query is required for a proper filtering by calculated fields
         */
        @SuppressWarnings("SqlNoDataSourceInspection")
        final String opaqueFetchQuery = """
             SELECT * FROM %s
                %s
                %s
        """.formatted(sqlFetchContext.getGenericQuery(), sqlFetchContext.getOrderClause(), sqlFetchContext.getPaginationClause());

        if (logger.isTraceEnabled()) {
            logger.trace("DataSource %s fetch query:\n%s\n\nparams:\n%s"
                .formatted(
                    getDataSource().getId(),
                    opaqueFetchQuery,
                    sqlFetchContext.getFilterData().stream()
                            .flatMap(fd -> sqlFetchContext.getFilterData().stream())
                            .flatMap(fd -> StreamSupport.stream(fd.values().spliterator(), false))
                            .map("%s"::formatted)
                            .collect(Collectors.joining(", "))
                )
            );
        }

        sqlFetchContext.setEffectiveSQL(opaqueFetchQuery);

        try(PreparedStatement st = connection.prepareStatement(opaqueFetchQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)){

            // If paginated, then tune statement accordingly
            if (sqlFetchContext.getPageSize() != -1) {
                st.setFetchSize(sqlFetchContext.getPageSize());
                st.setMaxRows(sqlFetchContext.getPageSize());
            }

            st.setFetchDirection(ResultSet.FETCH_FORWARD);

            int idx =0;
            for (IFilterData fd: sqlFetchContext.getFilterData()) {
                idx = fd.setStatementParameters(idx, st);
            }

            try (ResultSet rs = st.executeQuery() ) {
                while (rs.next())  {
                    final Map<String, Object> rowPkValues =  new HashMap<>();

                    final Object[] r = new Object[sqlFetchContext.getRequestedFields().size()];

                    int i =0;

                    // ORIGINAL FIELD ORDER MUST BE PRESERVED
                    for (DSField dsf: sqlFetchContext.getRequestedFields()) {
                        Object v = rs.getObject(i + 1);
                        if (rs.wasNull()) {
                            v = null;
                        }
                        r[i++] = v;

                        if (dsf.isPrimaryKey()) {
                            if (v == null) {
                                throw new ContextualRuntimeException(
                                        "Datasource '%s': Fetch failed, Primary Key value can not be null, but actually it is, field: '%s'."
                                                .formatted(
                                                    this.getDataSource().getId(),
                                                    dsf.getName()
                                                ),
                                        sqlFetchContext
                                );
                            }
                            rowPkValues.put(dsf.getName(), v);
                        }
                    }

                    for(int j = 0; j< sqlFetchContext.getRequestedFields().size(); ++j) {
                        final DSField dsf = sqlFetchContext.getRequestedFields().get(j);

                        r[j] = postProcessFieldValue(sqlFetchContext, rowPkValues, dsf, r[j]);

                        if (!isSubEntityFetchRequired(dsf)) {
                            continue;
                        }

                        /*
                         * Create EntitySubFetch for further processing
                         */
                        final List<ForeignRelation> ffs = sqlFetchContext.getAdditionalOutputs().get(dsf);
                        final List<DSField> requestedFields = ffs == null ? null : ffs.stream()
                                .map(ForeignRelation::field)
                                .collect(Collectors.toList());

                        r[j] = EntitySubFetch.create( !this.allowAdvancedCriteria(),
                                this::getDataSourceHandlerById,
                                this.getDataSource(),
                                dsf,
                                requestedFields,
                                rowPkValues
                        );
                    }

                    final Object[] postProcessed = postProcessRow(sqlFetchContext, r);

                    assert postProcessed.length == r.length;
                    data.add(postProcessed);
                }
            } catch (Throwable t) {
                if (t instanceof ContextualRuntimeException) {
                    throw t;
                }
                throw new ContextualRuntimeException("SQL fetch query execution failed.", t, sqlFetchContext);
            }
        }

        // -- Perform subsequent entity fetch requests, if any
        for(Object[] r: data) {
            for (int i=0; i< r.length; ++i) {
                if (r[i]  instanceof EntitySubFetch esf) {
                    r[i] = esf.fetch(connection);
                }
            }
        }


        return DSResponse.successFetch(request.getStartRow(), request.getStartRow() + data.size(), totalRows[0],
                sqlFetchContext.getRequestedFields(),
                data);
    }

    protected Object[] postProcessRow(SQLFetchContext<JDBCHandler> ctx, Object[] row) {
        return row;
    }

    protected Object postProcessFieldValue(SQLFetchContext<JDBCHandler> ctx, Map<String, Object> rowPkValues, DSField dsf, Object value) {
        return value;
    }

    protected  List<IFilterData> generateFilterData(
            DSRequest.OperationType operationType,
            DSRequest.TextMatchStyle textMatchStyle,
            IDSRequestData data,
            Predicate<String> exclusionPredicate ) {
        if (data instanceof Map) {
            return ((Map<String, Object>) data).entrySet()
                    .stream()
                    .filter( e -> !exclusionPredicate.test(e.getKey()) )
                    .map(e -> {
                        final DSField dsf = getField(e.getKey());

                        if (dsf == null) {
                            throw new RuntimeException("DataSource '%s': nothing known about field '%s'"
                                    .formatted(
                                            getDataSource().getId(),
                                            e.getKey()
                                    )
                            );
                        }

                        @SuppressWarnings("SwitchStatementWithTooFewBranches")
                        final Object value = switch (dsf.getType()) {
                            case TEXT -> switch (textMatchStyle) {
                                case EXACT -> e.getValue();

                                case SUBSTRING -> "%%%s%%".formatted(e.getValue());

                                default -> "%s%%".formatted(e.getValue());
                            };
                            default -> e.getValue();
                        };

                        final String filterStr;

                        if (TEXT.equals(dsf.getType()) && (!DSRequest.TextMatchStyle.EXACT.equals(textMatchStyle)) ) {
                            filterStr = "%s like ?";
                        } else {
                            filterStr = "%s = ?";
                        }

                        ForeignRelation effectiveField = determineEffectiveField(dsf);
                        effectiveField = effectiveField.createWithSqlFieldAlias(
                            operationType == DSRequest.OperationType.FETCH ?
                                formatColumnNameToAvoidAnyPotentialDuplication(
                                        effectiveField.dataSource(),
                                        effectiveField.field()
                                )
                                : effectiveField.field().getDbName());

                        return new FilterData(effectiveField, filterStr, value);
                    })
                    .collect(Collectors.toList());
        } else if (data == null){
            return List.of();
        } else {
            throw new IllegalStateException("DataSource '%s': data has unsupported format '%s'."
                    .formatted(
                            getDataSource().getId(),
                            data.getClass().getCanonicalName()
                    )
            );
        }
    }

    protected static String formatColumnNameToAvoidAnyPotentialDuplication(DataSource ds, DSField field) {
        return "%s_%s"
                .formatted(
                        field.getDbName(),
                        ds.getTableName()
                );
    }

    protected interface IFilterData {
        String sql();
        String sql(String aliasOrTable);

        Iterable<Object> values();

        default int setStatementParameters(int idx, PreparedStatement preparedStatement) throws SQLException {
            for (Object v : values()) {
                preparedStatement.setObject(++idx, v);
            }
            return idx;
        }

    }

    protected static class FilterData implements IFilterData{
        private final ForeignRelation dsFieldPair;
        private final String sqlTemplate;
        private transient String formattedSql;
        private final Object value;

        public FilterData(ForeignRelation dsFieldPair, String sqlTemplate, Object value) {
            this.dsFieldPair = dsFieldPair;
            this.sqlTemplate = sqlTemplate;
            this.value = value;
        }

        @SuppressWarnings("unused")
        public FilterData(ForeignRelation dsFieldPair, String sqlTemplate, Object... values) {
            this.dsFieldPair = dsFieldPair;
            this.sqlTemplate = sqlTemplate;
            this.value = values;
        }

        @Override
        public String sql() {
            if (formattedSql == null) {
                formattedSql = this.sql(dsFieldPair.dataSource().getTableName());
            }
            return formattedSql;
        }

        @Override
        public String sql(String aliasOrTable) {
            assert sqlTemplate != null;

            return sqlTemplate.formatted(
                dsFieldPair.formatAsSQL(aliasOrTable)
            );
        }

        public DSField field() {
            return dsFieldPair.field();
        }

        protected ForeignRelation getDsFieldPair() {
            return dsFieldPair;
        }

        public Iterable<Object> values() {
            return Collections.singletonList(value);
        }

        @Override
        public String toString() {
            return "FilterData{" +
                    "field=" + dsFieldPair +
                    ", sqlTemplate='" + sqlTemplate + '\'' +
                    ", value=" + value +
                    '}';
        }
    }

    public static abstract class AbstractSQLContext<H extends JDBCHandler> {
        private final H dsHandler;
        private final DSRequest request;
        private final OperationBinding operationBinding;

        public AbstractSQLContext(H dsHandler, DSRequest request, OperationBinding operationBinding) {
            this.request = request;
            this.operationBinding = operationBinding;
            this.dsHandler = dsHandler;
        }

        protected DataSource dataSource() {
            assert dsHandler != null;
            return dsHandler.dataSource();
        }

        protected H dsHandler() {
            assert dsHandler != null;
            return dsHandler;
        }

        public OperationBinding operationBinding() {
            return operationBinding;
        }

        protected DSRequest request() {
            assert request != null;
            return request;
        }

        protected static Predicate<String> createCriteriaExclusionPredicate(String excludeCriteriaFields) {
            final String[] splitted = excludeCriteriaFields == null ? null : excludeCriteriaFields.split("\\s*,\\s*");
            if ( splitted == null || splitted.length == 0 ) {
                return s -> false;
            } else {
                return new Predicate<>() {
                    final private Set<String> exclusions = new HashSet<>(Arrays.asList(splitted));

                    @Override
                    public boolean test(String s) {
                        return exclusions.contains(s);
                    }
                };
            }
        }

        public static Map<String, Object> convertFilterDataToMap(Collection<IFilterData> ifds) {
            final Map<String, Object> m = new HashMap<>();

            ifds.forEach(
                    ifd -> {
                        assert ifd instanceof FilterData;
                        final FilterData fd = (FilterData) ifd;

                        m.put(fd.field().getName(), fd.value);
                    }
            );

            return m;
        }

        /**
         * Wrapper to set table and field aliases at runtime in accordence to the alias auto generation rules
         */
        public interface ISQLForeignKeyRelation extends IForeignKeyRelation{
            String sourceTableAlias();
            String destTableAlias();

            static WrapperBuilder wrap(IForeignKeyRelation fkr) {
                return new WrapperBuilder(fkr);
            }

            static class WrapperBuilder {
                private final IForeignKeyRelation foreignKeyRelation;
                private String sourceTableAlias;
                private String destTableAlias;

                public WrapperBuilder(IForeignKeyRelation foreignKeyRelation) {
                    this.foreignKeyRelation = foreignKeyRelation;
                }

                public WrapperBuilder withSourceTableAlias(String sourceTableAlias) {
                    this.sourceTableAlias = sourceTableAlias;
                    return this;
                }

                public WrapperBuilder withDestFieldAlias(String destFieldAlias) {
                    this.destTableAlias = destFieldAlias;
                    return this;
                }

                public ISQLForeignKeyRelation wrap() {
                    return new ISQLForeignKeyRelation() {
                        private final IForeignKeyRelation foreignKeyRelation = WrapperBuilder.this.foreignKeyRelation;
                        private String sourceTableAlias = WrapperBuilder.this.sourceTableAlias;
                        private String destTableAlias = WrapperBuilder.this.destTableAlias;

                        @Override
                        public String sourceTableAlias() {
                            if (sourceTableAlias == null || sourceTableAlias.isBlank()) {
                                return dataSource().getTableName();
                            }
                            return this.sourceTableAlias;
                        }

                        @Override
                        public String destTableAlias() {
                            if (destTableAlias == null || destTableAlias.isBlank()) {
                                return foreign().dataSource().getTableName();
                            }
                            return this.destTableAlias;
                        }

                        @Override
                        public DataSource dataSource() {
                            return this.foreignKeyRelation.dataSource();
                        }

                        @Override
                        public DSField sourceField() {
                            return this.foreignKeyRelation.sourceField();
                        }

                        @Override
                        public boolean isInverse() {
                            return this.foreignKeyRelation.isInverse();
                        }

                        @Override
                        public ForeignRelation foreign() {
                            return this.foreignKeyRelation.foreign();
                        }
                    };
                }
            }
        }



        public static class JoinGenerator {

            private record JoinDBDescr(String joinType, String sourceTable, String sourceField, String destTableAlias, String destTable,
                                       String destField) {
            }

            private static String generateJoin(List<JoinDBDescr> jds) {

                // -- generate relation chain
                final StringBuilder sbld = new StringBuilder();

                for (JoinDBDescr jd : jds) {

                    final String effectiveDestTableAlias = jd.destTableAlias == null || jd.destTableAlias.isBlank() ?
                            null /* there is no alias */ : jd.destTableAlias;

                    sbld.append("%s JOIN %s %s ON %s.%s = %s.%s\n"
                            .formatted(
                                    jd.joinType,
                                    jd.destTable,
                                    effectiveDestTableAlias == null ? "" : effectiveDestTableAlias,
                                    jd.sourceTable, jd.sourceField,
                                    effectiveDestTableAlias == null ? jd.destTable : effectiveDestTableAlias, jd.destField
                            )
                    );
                }
                return sbld.toString();
            }
        }

        /**
         * @param frls
         * @param <FKR>
         * @return
         */
        public static <FKR extends IForeignKeyRelation>  String generateSQLJoin(List<FKR> frls) {
            // -- generate relation chain

            final String joinType = "LEFT";
            final List<JoinGenerator.JoinDBDescr> jds = new LinkedList<>();
            for (IForeignKeyRelation foreignKeyRelation : frls) {

                if(foreignKeyRelation.sourceField().isMultiple()) {
                    /**
                     *  Implementation is not perfect and will require re-work as more use cases will come.
                     *
                     *  The join direction is hardcoded and that is, probably, not a good idea but the fastest one
                     *  for the implementation.
                     */

                    /*
                     * It is many-to-many relation, the join, actually,  must be done over a 3rd table
                     */
                    if (foreignKeyRelation.sourceField().getJoinTable() == null) {
                        throw new IllegalStateException(
                            "DataSource '%s': field '%s.%s' marked as 'multiple: true', but does not specify any 'joinTable', relation: %s."
                                .formatted(
                                        foreignKeyRelation.dataSource().getId(),
                                        foreignKeyRelation.dataSource().getId(),
                                        foreignKeyRelation.sourceField().getName(),
                                        foreignKeyRelation
                                )
                        );
                    }

                    final DSField.JoinTableDescr joinTable = foreignKeyRelation.sourceField().getJoinTable();

                    // --- Handle dest Relation: dest -> joinTable
                    final DataSource dstDataSource = foreignKeyRelation.foreign().dataSource();

                    // Determine effective dest field
                    final DSField effectiveDstField = foreignKeyRelation.dataSource().getNonCompositePK();

                    final String effectiveDstTableAlias = foreignKeyRelation instanceof ISQLForeignKeyRelation sfkr ?
                            sfkr.destTableAlias() : dstDataSource.getTableName();

                    jds.add(new JoinGenerator.JoinDBDescr(
                                    "",
                                    effectiveDstTableAlias,
                                    effectiveDstField.getDbName(),
                                    "",
                                    joinTable.getTableName(),
                                joinTable.getDestColumn()
                            )
                    );

                    // --- Handle source Relation: joinTable -> src
                    final DataSource srcDataSource = foreignKeyRelation.dataSource();

                    // Determine effective source field
                    final DSField effectiveSrcField = srcDataSource.getNonCompositePK();

                    String effectiveSrcAlias = foreignKeyRelation instanceof ISQLForeignKeyRelation sfkr ?
                            sfkr.sourceTableAlias() : srcDataSource.getTableName();

                    jds.add(new JoinGenerator.JoinDBDescr(
                            "",
                            joinTable.getTableName(),
                            joinTable.getSourceColumn(),
                            effectiveSrcAlias,
                            srcDataSource.getTableName(),
                            effectiveSrcField.getDbName()
                        )
                    );

                } else {
                    /* */
                    final DataSource srcDataSource = foreignKeyRelation.dataSource();
                    final DataSource fkDataSource = foreignKeyRelation.foreign().dataSource();
                    final DSField fkField = foreignKeyRelation.foreign().field();
                    final DSField srcField = foreignKeyRelation.sourceField();

                    jds.add(new JoinGenerator.JoinDBDescr(
                            joinType,
                            srcDataSource.getTableName(),
                            srcField.getDbName(),
                            "",
                            fkDataSource.getTableName(),
                            fkField.getDbName()
                        )
                    );
                }
            }
            return JoinGenerator.generateJoin(jds);
        }
    }
}
