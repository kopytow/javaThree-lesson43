package org.example.dao;

import org.example.entity.Task;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.sql.DataSource;

public class TaskDao {
  private final DataSource dataSource;

  private static String INSERT_TASK = "INSERT INTO task (title, finished, created_date) VALUES (?, ?, ?)";
  private static String GET_ALL_TASK_SORTED_BY_ID = "SELECT * FROM task ORDER BY task_id";
  private static String GET_ALL_TASK_SORTED_BY_CREATED_DATE = "SELECT * FROM task ORDER BY created_date DESC";
  private static String GET_TASK_BY_ID = "SELECT * FROM task WHERE task_id = ?";
  private static String GET_ALL_NOT_FINISHED_TASK = "SELECT * FROM task WHERE finished = false";
  private static String SET_TASK_FINISHED = "UPDATE task SET finished = ?";
  private static String DELETE_ALL_TASK = "DELETE FROM task";
  private static String DELETE_TASK_BY_ID = "DELETE FROM task WHERE task_id = ?";

  public TaskDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public Task save(Task task) {
    // get connection
    // create statement
    // set params
    // execute
    // get id
    // set id

    try(
      Connection connection = dataSource.getConnection();
      PreparedStatement statement = connection.prepareStatement(INSERT_TASK, Statement.RETURN_GENERATED_KEYS)
      ) {

      statement.setString(1, task.getTitle());
      statement.setBoolean(2, task.getFinished());
      statement.setTimestamp(3, java.sql.Timestamp.valueOf(task.getCreatedDate()));
      statement.executeUpdate();

      try(ResultSet resultSet = statement.getGeneratedKeys()) {
        if (resultSet.next()) {
          task.setId(resultSet.getInt(1));
        }
      }

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }

    return task;
  }

  public List<Task> findAll() {
    List<Task> tasks = new ArrayList<>();

    try(Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(GET_ALL_TASK_SORTED_BY_ID)
        ) {

      while(resultSet.next()) {
        final Task task = new Task(
          resultSet.getString(2),
          resultSet.getBoolean(3),
          resultSet.getTimestamp(4).toLocalDateTime()
        );
        task.setId(resultSet.getInt(1));
        tasks.add(task);
      }

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }

    return tasks;
  }

  public int deleteAll() {
    try(Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()
    ) {
      return statement.executeUpdate(DELETE_ALL_TASK);
    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }
  }

  public Task getById(Integer id) {
    try(
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(GET_TASK_BY_ID)
    ) {

      statement.setInt(1, id);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet.next()) {
        var task = new Task(
                resultSet.getString("title"),
                resultSet.getBoolean("finished"),
                resultSet.getTimestamp("created_date").toLocalDateTime()
        );
        task.setId(resultSet.getInt("task_id"));
        return task;
      } else {return null;}

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }

  }

  public List<Task> findAllNotFinished() {
    var tasks = new ArrayList<Task>();

    try(
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(GET_ALL_NOT_FINISHED_TASK)
    ) {


      while (resultSet.next()) {
        var task = new Task(
                resultSet.getString("title"),
                resultSet.getBoolean("finished"),
                resultSet.getTimestamp("created_date").toLocalDateTime()
        );
        task.setId(resultSet.getInt("task_id"));
        tasks.add(task);
      }

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }
    return tasks;
  }

  public List<Task> findNewestTasks(Integer numberOfNewestTasks) {
    var tasks = new ArrayList<Task>();

    try(
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(GET_ALL_TASK_SORTED_BY_ID)
    ) {


      while (resultSet.next()) {
        var task = new Task(
                resultSet.getString("title"),
                resultSet.getBoolean("finished"),
                resultSet.getTimestamp("created_date").toLocalDateTime()
        );
        task.setId(resultSet.getInt("task_id"));
        tasks.add(task);
      }

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }
    return tasks.stream().sorted(Comparator.comparing(Task::getCreatedDate).reversed()).limit(numberOfNewestTasks).toList();
  }

  public Task finishTask(Task task) {
    try(
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(SET_TASK_FINISHED)
    ) {

      task.setFinished(true);
      statement.setBoolean(1, true);
      var result = statement.executeUpdate();

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }
    return task;
  }

  public void deleteById(Integer id) {
    try(Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(DELETE_TASK_BY_ID)
    ) {
      statement.setInt(1, id);
      statement.executeUpdate();
    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }
  }
}
