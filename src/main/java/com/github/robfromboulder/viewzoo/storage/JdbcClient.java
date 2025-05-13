package com.github.robfromboulder.viewzoo.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;


public class JdbcClient implements StorageClient {
    private final String jdbcUrl;
    private final Properties connectionProperties;
    private final ObjectMapper mapper;


    public JdbcClient(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.connectionProperties = new Properties();
        connectionProperties.setProperty("user", user);
        connectionProperties.setProperty("password", password);
        this.mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jdk8Module());
        initializeViewStore();
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, connectionProperties);
    }

    private void initializeViewStore() {
        try (Connection connection = getConnection()) {
            String createTableSql = "CREATE TABLE IF NOT EXISTS viewzoo (" +
                    "schema VARCHAR(255), " +
                    "view_name VARCHAR(255), " +
                    "definition TEXT, " +
                    "PRIMARY KEY (schema_name, view_name))";
            try (PreparedStatement statement = connection.prepareStatement(createTableSql)) {
                statement.execute();
            }
        } catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to initialize database");
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews() {
        String sql = "SELECT schema_name, view_name, definition FROM viewzoo";
        Map<SchemaTableName, ConnectorViewDefinition> viewDefinitions = new HashMap<>();

        try (Connection connection = getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            while (resultSet.next()) {
                String schemaName = resultSet.getString("schema_name");
                String viewName = resultSet.getString("view_name");
                String definition_text = resultSet.getString("definition");

                ConnectorViewDefinition def = mapper.readValue(definition_text, ConnectorViewDefinition.class);
                viewDefinitions.put(new SchemaTableName(schemaName, viewName), def);
            }

            return viewDefinitions;
        } catch (SQLException | JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to retrieve view definitions");
        }
    }

    @Override
    public void createView(String schema, String table, ConnectorViewDefinition definition) {

    }

    @Override
    public void dropView(String schema, String table) {

    }
}
