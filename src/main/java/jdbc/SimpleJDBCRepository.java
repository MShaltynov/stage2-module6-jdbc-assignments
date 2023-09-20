package jdbc;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class SimpleJDBCRepository {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Statement st = null;

    private static final String createUserSQL = "insert into myusers(firstname, lastname, age) values (?,?,?)";

    private static final String updateUserSQL = "UPDATE myusers SET firstname=?,lastname=?,age=? WHERE id =?";
    private static final String deleteUser = "DELETE FROM myusers where id =?";
    private static final String findUserByIdSQL = "SELECT * FROM myusers where id =?";
    private static final String findUserByNameSQL = "SELECT * FROM myusers where firstname =?";
    private static final String findAllUserSQL = "SELECT * FROM myusers";

    public Long createUser(User user) {
        try {
            connection = CustomDataSource.getInstance().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long id ;
        String name = user.getFirstName();
        String surName = user.getLastName();
        int age = user.getAge();
        if (name==null||surName==null){
            return null;
        }
        try {
            ps = connection.prepareStatement(createUserSQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, surName);
            ps.setInt(3, age);
            int affectedRows = ps.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("Creating user failed, no rows affected.");
            }

            try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    id = generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Creating user failed, no ID obtained.");
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }

    public User findUserById(Long userId) {
        String name = null;
        String surName = null;
        int age = 0;
        try {
            connection = CustomDataSource.getInstance().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            ps = connection.prepareStatement(findUserByIdSQL);
            ps.setLong(1, userId);
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                name = resultSet.getString("firstname");
                surName = resultSet.getString("lastname");
                age = resultSet.getInt("age");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new User(userId, name, surName, age);
    }

    public User findUserByName(String userName) {
        Long id = null;
        String name = null;
        String surName = null;
        int age = 0;
        try {
            connection = CustomDataSource.getInstance().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            ps = connection.prepareStatement(findUserByNameSQL);
            ps.setString(1, userName);
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                id = resultSet.getLong("id");
                name = resultSet.getString("firstname");
                surName = resultSet.getString("lastname");
                age = resultSet.getInt("age");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new User(id, name, surName, age);
    }

    public List<User> findAllUser() {
        List<User> users = new ArrayList<>();
        CustomConnector customConnector = new CustomConnector();
        connection = customConnector.getConnection("jdbc:postgresql://localhost:5432/myfirstdb");
        try {
            st = connection.createStatement();
            ResultSet resultSet = st.executeQuery(findAllUserSQL);
            while (resultSet.next()) {
                long id;
                String firstName;
                String lastName;
                int age;
                id = resultSet.getLong("id");
                firstName = resultSet.getString("firstName");
                lastName = resultSet.getString("lastName");
                age = resultSet.getInt("age");
                User user = new User(id, firstName, lastName, age);
                users.add(user);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    public User updateUser() {

        try {
            connection = CustomDataSource.getInstance().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long id = 5L;
        String name = "JohnUpdated";
        String surName = "Malkovich";
        int age = 12;
        Long updatedID = null;
        String updatedName = null;
        String updatedlastName = null;
        int updatedAge = 0;
        try {
            ps = connection.prepareStatement(updateUserSQL);
            ps.setString(1, name);
            ps.setString(2, surName);
            ps.setInt(3, age);
            ps.setLong(4, id);
            int affectedRows = ps.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("Creating user failed, no rows updated.");
            }
            System.out.println(affectedRows);
            ps = connection.prepareStatement(findUserByIdSQL);
            ps.setLong(1, id);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                updatedID = resultSet.getLong("id");
                updatedName = resultSet.getString("firstname");
                updatedlastName = resultSet.getString("lastname");
                updatedAge = resultSet.getInt("age");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new User(updatedID, updatedName, updatedlastName, updatedAge);
    }

    public void deleteUser(Long userId) {
        try {
            try {
                connection = CustomDataSource.getInstance().getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            ps = connection.prepareStatement(deleteUser);
            ps.setLong(1, userId);
            int i = ps.executeUpdate();
            if (i == 0) {
                System.out.println("there are no user with id " + userId);
            } else {
                System.out.println("user with id " + userId + " successfully deleted");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
