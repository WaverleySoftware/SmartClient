package org.srg.smartclient;

import org.srg.smartclient.isomorphic.DSRequest;
import org.srg.smartclient.isomorphic.OperationBinding;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SQLRemoveContext<H extends JDBCHandler> extends JDBCHandler.AbstractSQLContext {


    private String deleteSQL;

    private List<JDBCHandler.IFilterData> pkFieldData;

    public SQLRemoveContext(JDBCHandler dsHandler, DSRequest request, OperationBinding operationBinding) {
        super(dsHandler, request, operationBinding);
        init();
    }

    protected void init() {

        final Predicate<String> exclusionPredicate = s -> s.startsWith(dsHandler().getMetaDataPrefix());

        final List<JDBCHandler.IFilterData> filterData = dsHandler().generateFilterData(
                DSRequest.OperationType.REMOVE,
                DSRequest.TextMatchStyle.EXACT,
                request().getData(),
                exclusionPredicate
        );

        final Map<Boolean, List<JDBCHandler.IFilterData>> m = filterData.stream()
                .collect(Collectors.groupingBy((JDBCHandler.IFilterData fd) -> ((JDBCHandler.FilterData) fd).field().isPrimaryKey()));

        this.pkFieldData = m.get(Boolean.TRUE);


        final String whereSQL = pkFieldData.stream()
                .map(fd -> fd.sql())
                .collect(Collectors.joining("\n\t\t AND "));

        this.deleteSQL = switch (dataSource().getDeletionType()) {
            case DELETE -> """
                        DELETE FROM  %s
                        WHERE %s;                            
                    """.formatted(
                    dataSource().getTableName(),
                    whereSQL
            );
            case SOFT_DELETE -> """
                                        UPDATE  %s
                                        SET deleted = true
                                        WHERE %s;
                    """.formatted(
                    dataSource().getTableName(),
                    whereSQL
            );
            default -> throw new RuntimeException("DataSource '%s' not available for deletion"
                    .formatted(
                            dataSource().getId()
                    )
            );
        };

    }

    public String getDeleteSQL() {
        return deleteSQL;
    }

    public List<JDBCHandler.IFilterData> getPkFieldData() {
        return pkFieldData;
    }
}
