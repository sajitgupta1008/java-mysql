import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLAccess {

   private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Connection connect = null;
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    public void readDataBase(String host, String username, String password) throws Exception {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            String url = String.format("jdbc:mysql://%s/resbilling?user=%s&password=%s", host, username, password);

            connect = DriverManager.getConnection(url);

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();
            // Result set get the result of the SQL query
            resultSet = statement
                    .executeQuery("select * from resbilling.abc");
            writeResultSet(resultSet);

           /* // PreparedStatements can use variables and are more efficient
            preparedStatement = connect
                    .prepareStatement("insert into  feedback.comments values (default, ?, ?, ?, ? , ?, ?)");
            // "myuser, webpage, datum, summary, COMMENTS from feedback.comments");
            // Parameters start with 1
            preparedStatement.setString(1, "Test");
            preparedStatement.setString(2, "TestEmail");
            preparedStatement.setString(3, "TestWebpage");
            preparedStatement.setDate(4, new java.sql.Date(2009, 12, 11));
            preparedStatement.setString(5, "TestSummary");
            preparedStatement.setString(6, "TestComment");
            preparedStatement.executeUpdate();

            preparedStatement = connect
                    .prepareStatement("SELECT myuser, webpage, datum, summary, COMMENTS from feedback.comments");
            resultSet = preparedStatement.executeQuery();
            writeResultSet(resultSet);

            // Remove again the insert comment
            preparedStatement = connect
                    .prepareStatement("delete from feedback.comments where myuser= ? ; ");
            preparedStatement.setString(1, "Test");
            preparedStatement.executeUpdate();

            resultSet = statement
                    .executeQuery("select * from feedback.comments");
            writeMetaData(resultSet);*/

        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }

    }

    private void writeMetaData(ResultSet resultSet) throws SQLException {
        //  Now get some metadata from the database
        // Result set get the result of the SQL query

        System.out.println("The columns in the table are: ");

        System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            System.out.println("Column " + i + " " + resultSet.getMetaData().getColumnName(i));
        }
    }

    private void writeResultSet(ResultSet resultSet) throws SQLException {
        // ResultSet is initially before the first data set

        while (resultSet.next()) {
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1
            // e.g. resultSet.getSTring(2);

            Integer id = resultSet.getInt("id");
            String address = resultSet.getString("address");
            //Date date = resultSet.getDate("datum");
            JsonNode jsonNode;
            try {
               jsonNode =  OBJECT_MAPPER.readTree(address);
            } catch (IOException ex) {
                System.out.println("Parsing failed for record id : " + id);
                return;
            }

            System.out.println("User: " + id);
            System.out.println("Website: " + jsonNode.path("a").textValue());
            System.out.println("summary: " + jsonNode.path("b").textValue());
            System.out.println("Date: " + jsonNode.path("c").textValue());

            PreparedStatement statement = connect.prepareStatement("insert into xyz(i, a, b) VALUES(?,?,?)");
            statement.setInt(1, id);
            statement.setString(2, jsonNode.path("a").textValue());
            statement.setString(3, jsonNode.path("b").textValue());

            statement.execute();
        }
    }

    // You need to close the resultSet
    private void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {

        }
    }

}