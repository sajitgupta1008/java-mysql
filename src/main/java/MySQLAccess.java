import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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
    private ResultSet resultSet = null;

    public void readCategoryArticles(String host, String username, String password) throws Exception {
        // This will load the MySQL driver, each DB has its own driver
        Class.forName("com.mysql.cj.jdbc.Driver");
        // Setup the connection with the DB
        String url = String.format("jdbc:mysql://%s/resbilling?user=%s&password=%s", host, username, password);

        connect = DriverManager.getConnection(url);

        // Statements allow to issue SQL queries to the database
        statement = connect.createStatement();
        // Result set get the result of the SQL query
        resultSet = statement
                .executeQuery("select * from resbilling.CATEGORY_ARTICLE");
        this.writeCategoryArticles(resultSet);
    }

    public void readCareerArticles() throws SQLException {
        statement = connect.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from resbilling.CAREER_ARTICLE");
        this.writeCareerArticles(resultSet);
    }

    private void writeCareerArticles(ResultSet resultSet) throws SQLException {

        PreparedStatement statement = connect.prepareStatement("insert into resbilling.ARTICLE(TYPE, TYPE_ID," +
                " TITLE, DESCRIPTION, URL, INTERVIEW) VALUES(?,?,?,?,?,?)");
        int i = 0;

        while (resultSet.next()) {
            int careerId = resultSet.getInt("CAREER_ID");
            String title = resultSet.getString("TITLE");
            String description = resultSet.getString("DESCRIPTION");
            String url = resultSet.getString("URL");
            String interview = resultSet.getString("INTERVIEW");

            statement.setString(1, "career");
            statement.setInt(2, careerId);
            statement.setString(3, title);
            statement.setString(4, description);
            statement.setString(5, url);
            statement.setString(6, interview);
            statement.execute();
            i++;
        }

        System.out.println("Inserted " + i + " career articles.");
    }

    private void writeCategoryArticles(ResultSet resultSet) throws SQLException {
        // ResultSet is initially before the first data set
        int i = 0;
        while (resultSet.next()) {

            int pageId = resultSet.getInt("PAGE_ID");
            int id = resultSet.getInt("ID");
            String value = resultSet.getString("VALUE");

            JsonNode jsonNode;

            try {
                jsonNode = OBJECT_MAPPER.readTree(value);
            } catch (IOException ex) {
                System.out.println("Parsing failed for record id : " + id);
                ex.printStackTrace();
                continue;
            }

            ArrayNode articles = (ArrayNode) jsonNode.get("Description");
            System.out.println("Fetched record with ID: " + id + ", articles: " + articles);

            PreparedStatement statement = connect.prepareStatement("insert into resbilling.ARTICLE(TYPE, TYPE_ID," +
                    " TITLE, DESCRIPTION, URL, INTERVIEW) VALUES(?,?,?,?,?,?)");

            for (JsonNode article : articles) {
                statement.setString(1, "category");
                statement.setInt(2, pageId);
                statement.setString(3, article.path("articleTitle").textValue());
                statement.setString(4, article.path("articleDesc").textValue());
                statement.setString(5, article.path("articleUrl").textValue());
                statement.setString(6, "N");
                statement.addBatch();
                i++;
            }

            statement.executeBatch();
            System.out.println("Inserted category articles with ID " + id);
        }

        System.out.println("Inserted " + i + " articles.");
    }

    // You need to close the resultSet
    public void close() {
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