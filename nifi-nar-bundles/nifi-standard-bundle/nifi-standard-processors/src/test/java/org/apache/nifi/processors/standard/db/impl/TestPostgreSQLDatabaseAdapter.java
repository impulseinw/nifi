package org.apache.nifi.processors.standard.db.impl;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPostgreSQLDatabaseAdapter {
    private PostgreSQLDatabaseAdapter testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new PostgreSQLDatabaseAdapter();
    }

    @Test
    public void testSupportsUpsert() throws Exception {
        assertTrue(testSubject.getClass().getSimpleName() + " should support upsert", testSubject.supportsUpsert());
    }

    @Test
    public void testGetUpsertStatementWithNullTableName() throws Exception {
        testGetUpsertStatement(null, Arrays.asList("notEmpty"), Arrays.asList("notEmpty"), new IllegalArgumentException("Table name cannot be null or blank"));
    }

    @Test
    public void testGetUpsertStatementWithBlankTableName() throws Exception {
        testGetUpsertStatement("", Arrays.asList("notEmpty"), Arrays.asList("notEmpty"), new IllegalArgumentException("Table name cannot be null or blank"));
    }

    @Test
    public void testGetUpsertStatementWithNullColumnNames() throws Exception {
        testGetUpsertStatement("notEmpty", null, Arrays.asList("notEmpty"), new IllegalArgumentException("Column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatementWithEmptyColumnNames() throws Exception {
        testGetUpsertStatement("notEmpty", Collections.emptyList(), Arrays.asList("notEmpty"), new IllegalArgumentException("Column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatementWithNullKeyColumnNames() throws Exception {
        testGetUpsertStatement("notEmpty", Arrays.asList("notEmpty"), null, new IllegalArgumentException("Key column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatementWithEmptyKeyColumnNames() throws Exception {
        testGetUpsertStatement("notEmpty", Arrays.asList("notEmpty"), Collections.emptyList(), new IllegalArgumentException("Key column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatement() throws Exception {
        // GIVEN
        String tableName = "table";
        List<String> columnNames = Arrays.asList("column1","column2", "column3", "column4");
        Collection<String> uniqueKeyColumnNames = Arrays.asList("column2","column4");

        String expected = "INSERT INTO" +
            " table(column1, column2, column3, column4) VALUES (?, ?, ?, ?)" +
            " ON CONFLICT (column2, column4)" +
            " DO UPDATE SET" +
            " (column1, column2, column3, column4) = (EXCLUDED.column1, EXCLUDED.column2, EXCLUDED.column3, EXCLUDED.column4)";

        // WHEN
        // THEN
        testGetUpsertStatement(tableName, columnNames, uniqueKeyColumnNames, expected);
    }

    private void testGetUpsertStatement(String tableName, List<String> columnNames, Collection<String> uniqueKeyColumnNames, IllegalArgumentException expected) {
        try {
            testGetUpsertStatement(tableName, columnNames, uniqueKeyColumnNames, (String)null);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(expected.getMessage(), e.getMessage());
        }
    }

    private void testGetUpsertStatement(String tableName, List<String> columnNames, Collection<String> uniqueKeyColumnNames, String expected) {
        // WHEN
        String actual = testSubject.getUpsertStatement(tableName, columnNames, uniqueKeyColumnNames);

        // THEN
        assertEquals(expected, actual);
    }
}
