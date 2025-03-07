package db;
import java.sql.*;
import java.util.Scanner;

public class DB_Access {
	static Scanner scanner = new Scanner(System.in);
	static String dbName;
	static String tableName;
	static String userName; 
    static String userPass; 
       
    public static boolean createDatabase(String _dbName) {
        Connection conn = null;
        Statement stat = null;
        
        dbName = _dbName;

        String url = "jdbc:postgresql://localhost:5432/"; 
        
        boolean result = false;
        
        if (!dbName.isEmpty()) {
	        try {
	            Class.forName("org.postgresql.Driver");
	            conn = DriverManager.getConnection(url, "postgres", "Pass1");
	            stat = conn.createStatement();
	            
	            String sql = "CREATE DATABASE " + dbName;
	            stat.executeUpdate(sql);
	
	            initializeDatabase();
	            
	            createRoles();
	            
	            System.out.println("The database " + dbName + " is successfully created.");
	            
	            result = true;
	        } 
	        catch (SQLException | ClassNotFoundException e) {
	            System.out.println("Error when creating the database: " + e.getMessage());
	        } 
	        finally {
	            try {
	                if (stat != null) stat.close();
	                if (conn != null) conn.close();
	            }
	            catch (SQLException e) {
	                e.printStackTrace();
	            }
	        }
        }
        else {
            System.out.println("Database name cannot be empty.");
        }
        return result;
    }
    
    public static boolean openDatabase(String _dbName) {
        Connection conn = null;
        Statement stat = null;
        
        dbName = _dbName;

        String url = "jdbc:postgresql://localhost:5432/" + dbName; 
        
        boolean result = false;
        
        if (!dbName.isEmpty()) {
	        try {
	            Class.forName("org.postgresql.Driver");
	            conn = DriverManager.getConnection(url, "postgres", "Pass1");
	            stat = conn.createStatement();
	            
	            initializeDatabase();
	            
	            createRoles();
	
	            System.out.println("The database " + dbName + " is successfully opened.");
	            
	            result = true;
	        } 
	        catch (SQLException | ClassNotFoundException e) {
	            System.out.println("Error when opening the database: " + e.getMessage());
	        } 
	        finally {
	            try {
	                if (stat != null) stat.close();
	                if (conn != null) conn.close();
	            }
	            catch (SQLException e) {
	                e.printStackTrace();
	            }
	        }
        }
        else {
            System.out.println("Database name cannot be empty.");
        }
        return result;
    }
    
    public static void initializeDatabase() {
        String url = "jdbc:postgresql://localhost:5432/" + dbName;
        
        try (Connection conn = DriverManager.getConnection(url, "postgres", "Pass1");
             Statement stat = conn.createStatement()) {
        	
            String CreateTableProcedure = "CREATE OR REPLACE FUNCTION create_table(table_name TEXT) RETURNS VOID AS $$ " 
            		+ "BEGIN " 
            		+ "		EXECUTE format('CREATE TABLE IF NOT EXISTS %I (" 
            		+ "		id INT PRIMARY KEY CHECK(id > 0), " 
            		+ "		name VARCHAR(100) NOT NULL, " 
            		+ "		group_number INT NOT NULL CHECK (group_number > 0), " 
            		+ "		coach VARCHAR(100) NOT NULL, " 
            		+ "		number_classes INT NOT NULL CHECK (number_classes >= 0), " 
            		+ "		record FLOAT NOT NULL CHECK (record >= 0))', table_name); " 
            		+ "END; $$ LANGUAGE plpgsql;";
            stat.execute(CreateTableProcedure);
            
            String InsertDataProcedure = "CREATE OR REPLACE FUNCTION insert_data(" 
            		+ "table_name TEXT, p_id INT, p_name VARCHAR, p_group_number INT, p_coach VARCHAR, p_number_classes INT, p_record FLOAT) " 
            		+ "RETURNS VOID AS $$ BEGIN " 
            		+ "		EXECUTE format('INSERT INTO %I (id, name, group_number, coach, number_classes, record) "
            		+ "		VALUES ($1, $2, $3, $4, $5, $6)', table_name) " 
                    + "		USING p_id, p_name, p_group_number, p_coach, p_number_classes, p_record; " 
                    + "END; $$ LANGUAGE plpgsql;";
            stat.execute(InsertDataProcedure);
            
            String ClearTableProcedure = "CREATE OR REPLACE FUNCTION clear_table(table_name TEXT) "
            		+ "RETURNS VOID AS $$ " 
            		+ "BEGIN " 
                    + "		EXECUTE format('TRUNCATE TABLE %I RESTART IDENTITY CASCADE', table_name); "
                    + "END; $$ LANGUAGE plpgsql;";
            stat.execute(ClearTableProcedure);
            
            String DropDataProcedure = "CREATE OR REPLACE FUNCTION drop_database(db_name TEXT) "
            		+ "RETURNS VOID AS $$ "
            		+ "BEGIN "
            		+ "    EXECUTE format('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %L', db_name); "
            		+ "    EXECUTE format('DROP DATABASE IF EXISTS %I', db_name); "
            		+ "END; "
            		+ "$$ LANGUAGE plpgsql;";
            stat.execute(DropDataProcedure);

            String UpdateStudentProcedure = "CREATE OR REPLACE FUNCTION update_student_by_id( "
            		+ "    table_name TEXT, "
            		+ "    p_id INT, "
            		+ "    p_name TEXT, "
            		+ "    p_group_number INT, "
            		+ "    p_coach TEXT, "
            		+ "    p_number_classes INT, "
            		+ "    p_record FLOAT "
            		+ ") RETURNS VOID AS $$ "
            		+ "BEGIN "
            		+ "    EXECUTE format( "
            		+ "        'UPDATE %I SET name = $2, group_number = $3, coach = $4, number_classes = $5, record = $6 WHERE id = $1', "
            		+ "        table_name "
            		+ "    ) USING p_id, p_name, p_group_number, p_coach, p_number_classes, p_record; "
            		+ "END; $$ LANGUAGE plpgsql; ";
            stat.execute(UpdateStudentProcedure);

            String SearchByNameProcedure = "CREATE OR REPLACE FUNCTION search_by_name(\r\n"
            		+ "    table_name TEXT, "
            		+ "    search_text TEXT "
            		+ ") RETURNS TABLE( "
            		+ "    id INT, "
            		+ "    name VARCHAR, "
            		+ "    group_number INT, "
            		+ "    coach VARCHAR, "
            		+ "    number_classes INT, "
            		+ "    record FLOAT "
            		+ ") AS $$ "
            		+ "BEGIN\r\n"
            		+ "    RETURN QUERY EXECUTE format( "
            		+ "        'SELECT * FROM %I WHERE name = $1', "
            		+ "        table_name"
            		+ "    ) USING search_text; "
            		+ "END; $$ LANGUAGE plpgsql;";
            stat.execute(SearchByNameProcedure);
            
            String DeleteByNameProcedure = "CREATE OR REPLACE FUNCTION delete_by_name( "
            		+ "    table_name TEXT, "
            		+ "    search_text TEXT "
            		+ ") RETURNS VOID AS $$ "
            		+ "BEGIN "
            		+ "    EXECUTE format( "
            		+ "        'DELETE FROM %I WHERE name = $1', "
            		+ "        table_name "
            		+ "    ) USING search_text; "
            		+ "END; $$ LANGUAGE plpgsql;";
            stat.execute(DeleteByNameProcedure);
			
            System.out.println("Stored procedures are successfully created.");

        } catch (SQLException e) {
            System.out.println("Error while initializing database: " + e.getMessage());
        }
    }

    public static void createRoles() {
        Connection conn = null;
        Statement stat = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;

        String createRolesSQL = "DO $$ DECLARE db_name TEXT := current_database(); BEGIN "
        		+ "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'guest') THEN "
                + "EXECUTE format('CREATE ROLE guest LOGIN PASSWORD ''pass3'''); "
                + "END IF; "
                + "EXECUTE format('GRANT CONNECT ON DATABASE %I TO guest', db_name); "
                + "EXECUTE format('GRANT USAGE ON SCHEMA public TO guest'); "
                + "EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO guest'); "

                
				+ "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coach') THEN "
                + "EXECUTE format('CREATE ROLE coach LOGIN PASSWORD ''pass2'''); "
                + "END IF; "
                + "EXECUTE format('GRANT CONNECT ON DATABASE %I TO coach', db_name); "
                + "EXECUTE format('GRANT USAGE ON SCHEMA public TO coach'); "
                + "EXECUTE format('GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO coach'); "
                
				+ "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'manager') THEN "
                + "EXECUTE format('CREATE ROLE manager LOGIN PASSWORD ''pass1'''); "
                + "END IF; "
                + "EXECUTE format('GRANT CONNECT ON DATABASE %I TO manager', db_name); "
                + "EXECUTE format('GRANT USAGE ON SCHEMA public TO manager'); "
                + "EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO manager'); "
                + "EXECUTE format('GRANT CREATE ON SCHEMA public TO manager'); "

                + "END $$;";
        
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, "postgres", "Pass1");
            stat = conn.createStatement();
            stat.execute(createRolesSQL);

            System.out.println("Roles and privileges have been successfully created.");
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Error when creating roles: " + e.getMessage());
        } finally {
            try {
                if (stat != null) stat.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean tryToConnect(String _userName, String _userPass) {
    	Connection conn = null;
        Statement stat = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;
        
        boolean result = false;
        
    	try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, _userName, _userPass);
            stat = conn.createStatement();
            
            result = true;
        }
        catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } 
        finally {
            try {
                if (stat != null) stat.close();
                if (conn != null) conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    
    public static boolean createTable(String _tableName) {
        Connection conn = null;
        CallableStatement stat = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;
        
        boolean result = false;
        
    	tableName = _tableName;;
        
        if (!tableName.isEmpty()) {
            try {
                Class.forName("org.postgresql.Driver");
                conn = DriverManager.getConnection(url, userName, userPass);
                
                stat = conn.prepareCall("{ call create_table(?) }");
                
                stat.setString(1, tableName);
                stat.execute();
                System.out.println("The table " + tableName + " is successfully created.");
                result = true;
            } catch (SQLException | ClassNotFoundException e) {
                System.out.println("Error when creating the table: " + e.getMessage());
            } finally {
                try {
                    if (stat != null) stat.close();
                    if (conn != null) conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Table name cannot be empty.");
        }
        return result;
    }
    
    public static boolean openTable(String _tableName) {
        Connection conn = null;
        CallableStatement stat = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;
        
        boolean result = false;
        
    	tableName = _tableName;;
        
        if (!tableName.isEmpty()) {
            try {
                Class.forName("org.postgresql.Driver");
                conn = DriverManager.getConnection(url, userName, userPass);
                
                System.out.println("The table " + tableName + " is successfully opened.");
                result = true;
            } catch (SQLException | ClassNotFoundException e) {
                System.out.println("Error when creating the table: " + e.getMessage());
            } finally {
                try {
                    if (stat != null) stat.close();
                    if (conn != null) conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Table name cannot be empty.");
        }
        return result;
    }

    public static boolean insertData(int id, String name, int groupNumber, String coach, int numberClasses, float record) {
        Connection conn = null;
        CallableStatement cstat = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;

        boolean result = false;

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, userName, userPass);
            
            cstat = conn.prepareCall("{ call insert_data(?, ?, ?, ?, ?, ?, ?) }");

            while (true) {
                System.out.println("\nEnter student data (or type 'exit' to finish): ");

                cstat.setString(1, tableName);  
                cstat.setInt(2, id);            
                cstat.setString(3, name);      
                cstat.setInt(4, groupNumber);   
                cstat.setString(5, coach);     
                cstat.setInt(6, numberClasses); 
                cstat.setFloat(7, record);      

                cstat.execute();
                System.out.println("Student is successfully added!");

                result = true;
            }
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Error when inserting data: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.out.println("Invalid input format. Please enter correct values.");
        } finally {
            try {
                if (cstat != null) cstat.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    } 
    
    
    
    public static void dropDatabase(String dbName) {
        Connection conn = null;
        CallableStatement cstat = null;
        PreparedStatement terminateStmt = null;
        ResultSet rs = null;

        String url = "jdbc:postgresql://localhost:5432/postgres"; 

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, "postgres", "Pass1");

            String checkDBExistsSQL = "SELECT 1 FROM pg_database WHERE datname = ?";
            PreparedStatement checkStmt = conn.prepareStatement(checkDBExistsSQL);
            checkStmt.setString(1, dbName);
            rs = checkStmt.executeQuery();

            if (!rs.next()) {
                System.out.println("Database " + dbName + " does not exist.");
                return;
            }

            String terminateSQL = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = ?";
            terminateStmt = conn.prepareStatement(terminateSQL);
            terminateStmt.setString(1, dbName);
            terminateStmt.execute();
            terminateStmt.close();

            cstat = conn.prepareCall("{ call drop_database(?) }");
            cstat.setString(1, dbName);
            cstat.executeUpdate();

            System.out.println("The database " + dbName + " has been deleted.");
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Error when deleting the database: " + e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
                if (terminateStmt != null) terminateStmt.close();
                if (cstat != null) cstat.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static boolean clearTable() {
        Connection conn = null;
        CallableStatement cstat = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;
        
        boolean result = false;

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, userName, userPass);

            cstat = conn.prepareCall("{ call clear_table(?) }");
            cstat.setString(1, tableName);
            cstat.execute();

            System.out.println("Table " + tableName + " has been cleared.");
            
            result = true;
        }
        catch (SQLException | ClassNotFoundException e) {
            System.out.println("Error clearing table: " + e.getMessage());
        }
        finally {
            try {
                if (cstat != null) cstat.close();
                if (conn != null) conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    
    public static boolean updateStudent(int studentId, String newName, int newGroupNumber, String newCoach, int newNumberClasses, float newRecord) {
        Connection conn = null;
        CallableStatement cstat = null;
        PreparedStatement checkStmt = null;
        ResultSet rs = null;
        boolean result = false;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, userName, userPass);

            String checkSQL = "SELECT COUNT(*) FROM " + tableName + " WHERE id = ?";
            checkStmt = conn.prepareStatement(checkSQL);
            checkStmt.setInt(1, studentId);
            rs = checkStmt.executeQuery();

            if (rs.next() && rs.getInt(1) > 0) {
                cstat = conn.prepareCall("{ call update_student_by_id(?, ?, ?, ?, ?, ?, ?) }");
                cstat.setString(1, tableName);
                cstat.setInt(2, studentId);
                cstat.setString(3, newName);
                cstat.setInt(4, newGroupNumber);
                cstat.setString(5, newCoach);
                cstat.setInt(6, newNumberClasses);
                cstat.setFloat(7, newRecord);

                cstat.executeUpdate();
                System.out.println("Student updated successfully!");
                result = true;
            } else {
                System.out.println("No student found with this ID.");
            }
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Error updating student: " + e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
                if (checkStmt != null) checkStmt.close();
                if (cstat != null) cstat.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    
    public static String searchByName(String searchValue) {
        Connection conn = null;
        CallableStatement cstat = null;
        ResultSet rs = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, userName, userPass);

            cstat = conn.prepareCall("{ call search_by_name(?, ?) }");
            cstat.setString(1, tableName);
            cstat.setString(2, searchValue);

            rs = cstat.executeQuery();
            StringBuilder result = new StringBuilder("Results:\n");

            boolean found = false;
            while (rs.next()) {
                found = true;
                result.append("ID: ").append(rs.getInt("id"))
                      .append(", Name: ").append(rs.getString("name"))
                      .append(", Group: ").append(rs.getInt("group_number"))
                      .append(", Coach: ").append(rs.getString("coach"))
                      .append(", Classes: ").append(rs.getInt("number_classes"))
                      .append(", Record: ").append(rs.getFloat("record")).append("\n");
            }
            if (!found) {
            	System.out.println("No students found.");
            	return "No students found.";
            } 
            else {
            	System.out.println(result.toString());
            	return result.toString();
            }
        } catch (SQLException | ClassNotFoundException e) {
        	System.out.println("Error searching: " + e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
                if (cstat != null) cstat.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "Search Error";
    }

    
    public static void deleteByName(String deleteName) {
        Connection conn = null;
        CallableStatement cstat = null;

        String url = "jdbc:postgresql://localhost:5432/" + dbName;

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, userName, userPass);

            cstat = conn.prepareCall("{ call delete_by_name(?, ?) }");
            cstat.setString(1, tableName);
            cstat.setString(2, deleteName);

            int affectedRows = cstat.executeUpdate();

            if (affectedRows > 0) {
            	System.out.println("Student deleted successfully!");
            } else {
            	System.out.println("No student found with this name.");
            }
        } catch (SQLException | ClassNotFoundException e) {
        	System.out.println("Error deleting student: " + e.getMessage());
        } finally {
            try {
                if (cstat != null) cstat.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    
    
}