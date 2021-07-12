package org.srg.smartclient;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.srg.smartclient.utils.Serde;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;

public abstract class AbstractJDBCHandlerTest<H extends JDBCHandler> extends AbstractHandlerTest<H> {
    protected static class ExtraField extends AbstractHandlerTest.ExtraFieldBase{

        public static String OneToMany_FetchEntireEntities = Employee_RolesFromEmployeeRole;

        public static String OneToMany_FetchOnlyIds = """
                [
                    {
                        name:"roles"
                        ,foreignKey:"EmployeeRoleDS.employee"
                        ,multiple:true
                    }
                ]""";

        public static String Project_IncludeManagerEmailFromEmployee = """
                [
                     {
                         name:"manager_email"
                         ,type:"TEXT"
                         ,includeFrom:"EmployeeDS.email"
                         ,includeVia:"manager"
                         ,canEdit:false
                         ,hidden:false
                     }
                ]""";

        public static String Project_IncludeFromClient = """
                [
                    {
                        name:"client"
                        ,type: "INTEGER"
                        ,foreignKey:"ClientDS.id"
                        ,displayField:"clientName"
                        ,foreignDisplayField:"name"
                        ,dbName:"client_id"
                        ,canEdit:false
                    },
                    {
                        name:"clientName"
                        ,type:"TEXT"
                        ,includeFrom:"ClientDS.name"
                        ,includeVia:"client"
                        ,canEdit:false
                        ,hidden:true
                    }
                ]""";

        public static String IncludeFromLocation = """
                [
                    {
                        name: 'location'
                        ,dbName: 'location_id'
                        ,type: "INTEGER"
                        ,foreignKey:"LocationDS.id"
                        ,displayField:"locationCity"
                    },
                    {
                        name:"locationCity"
                        ,type:"TEXT"
                        ,includeFrom:"LocationDS.city"
                    }
                ]""";

        public static String Email = """
                [
                    {
                        name: "email",
                        type: "text"
                    }
                ]
                """;

        public static String Deleted = """
                 [
                    {
                        name: "deleted",
                        type: "boolean"
                    }
                ]
                """;

        public static String FiredAt = """
                [
                    {
                        name: "firedAt",
                        type: "datetime"
                    }
                ]""";

        public static String SqlCalculated = """
                [
                    {
                        name: "calculated",
                        type: "text",
                        customSelectExpression: "CONCAT(employee.id, '_', employee.name)"
                    }
                ]
                """;

        private ExtraField(){}
    }

    protected JdbcDataSource jdbcDataSource = new JdbcDataSource();
    private Connection connection;
    private JDBCHandler.JDBCPolicy jdbcPolicy;


    @BeforeAll
    public static void setupObjectMapper() {
        JsonTestSupport.defaultMapper = Serde.createMapper();
    }

    @AfterAll
    public static void shutdownDB() {
    }

    protected Void initDB() {
        Flyway flyway = new Flyway(
                new FluentConfiguration()
                        .dataSource(jdbcDataSource)
                        .locations("classpath:db")
        );

        flyway.clean();
        flyway.migrate();

        return null;
    }

    @Override
    protected JDBCHandler.JDBCPolicy getJDJdbcPolicy() {
        return (db, callback) -> {
            jdbcPolicy.withConnectionDo(db, callback);
        };
    }

    @BeforeEach
    public void setupDataSources() throws Exception {

        // SET TZ to UTC, otherwise timestamps stored in DB as GMT will be treated as local timezone
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        jdbcDataSource.setURL("jdbc:h2:mem:test:˜/test;DB_CLOSE_DELAY=0;AUTOCOMMIT=OFF;database_to_lower=true");
        jdbcDataSource.setUser("sa");
        jdbcDataSource.setPassword("sa");

        connection = jdbcDataSource.getConnection();
        initDB();
        connection.commit();

        jdbcPolicy = (db, callback) -> {
            try (Connection conn = jdbcDataSource.getConnection()) {
                callback.apply(conn);
            }
        };
    }

    @AfterEach
    public void closeDbConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }

        connection = null;
    }

}
