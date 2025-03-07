package db;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import java.sql.*;

public class MainWindow extends JFrame {
    private JButton btnCreateDB, btnOpenDB, btnCreateTable, btnInsertData, btnDropDB, btnClearTable, btnUpdateStudent, btnSearchByName, btnDeleteByName;
    private String dbName;
    private String dbTable;
    private String userLogin;
    private String userPassword;
    private JTable table;
    private DefaultTableModel tableModel;

    public MainWindow() {
        setTitle("Karting School Database");
        setSize(800, 600);
        
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new FlowLayout());

        btnCreateDB = new JButton("Create database");
        btnOpenDB = new JButton("Open database");
        btnCreateTable = new JButton("Create table");
        btnInsertData = new JButton("Insert data");
        btnDropDB = new JButton("Drop database");
        btnClearTable = new JButton("Clear table");

        btnUpdateStudent = new JButton("Update student info");
        btnSearchByName = new JButton("Search by name");
        btnDeleteByName = new JButton("Delete by name");

        btnCreateDB.addActionListener(e -> createDatabase());
        btnOpenDB.addActionListener(e -> openDatabase());
        btnCreateTable.addActionListener(e -> createTable());
        btnInsertData.addActionListener(e -> insertData());
        btnDropDB.addActionListener(e -> dropDatabase());
        btnClearTable.addActionListener(e -> clearTable());
        btnUpdateStudent.addActionListener(e -> updateStudent());
        btnSearchByName.addActionListener(e -> searchByName());
        btnDeleteByName.addActionListener(e -> deleteByName());

        add(btnCreateDB);
        add(btnOpenDB);
        
        tableModel = new DefaultTableModel();
        table = new JTable(tableModel);
        JScrollPane scrollPane = new JScrollPane(table);
        scrollPane.setPreferredSize(new Dimension(780, 400));
        add(scrollPane);
    }

    private void createDatabase() {
        dbName = JOptionPane.showInputDialog(this, "Enter database name:");
        if (dbName != null && !dbName.trim().isEmpty()) {
            if (DB_Access.createDatabase(dbName)){
            	JOptionPane.showMessageDialog(this, "Database " + dbName + " created.");
            	while(!login()){
            	}
            }
        }
        else{
        	JOptionPane.showMessageDialog(this, "Couldn't create database", "Error", JOptionPane.ERROR_MESSAGE);
        }
    }
    
    private void openDatabase() {
        dbName = JOptionPane.showInputDialog(this, "Enter database name:");
        if (dbName != null && !dbName.trim().isEmpty()) {
            DB_Access.openDatabase(dbName);
            while(!login()){
            }
            if(openTable()){
            	printData();
            }
        }
        else{
        JOptionPane.showMessageDialog(this, "Couldn't open database", "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private boolean login() {
    	boolean flag = false;
    	
        JPanel panel = new JPanel();
    	JTextField userLoginField = new JTextField(20);
    	JPasswordField userPasswordField = new JPasswordField(20);

    	panel.add(new JLabel("Enter username (guest, coach, manager or others):"));
    	panel.add(userLoginField);
    	panel.add(new JLabel("Enter password:"));
   	 	panel.add(userPasswordField);

    	int result = JOptionPane.showConfirmDialog(this, panel, "Login", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);

    	if (result == JOptionPane.OK_OPTION) {
        	String userLogin = userLoginField.getText();
        	String userPassword = new String(userPasswordField.getPassword());

        if (userLogin == null || userLogin.trim().isEmpty() || userPassword == null || userPassword.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Both fields must be filled.", "Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        
		DB_Access.userName = userLogin;
        DB_Access.userPass = userPassword;
        
        if(DB_Access.tryToConnect(userLogin, userPassword)){
        	flag = true;
            add(btnCreateTable);
            add(btnInsertData);
            add(btnDropDB);
            add(btnClearTable);
            add(btnUpdateStudent);
            add(btnSearchByName);
            add(btnDeleteByName);
        }
        else{
        	JOptionPane.showMessageDialog(this, "Couldn't connect", "Error", JOptionPane.ERROR_MESSAGE);
		}

        revalidate();
        repaint();
		}
		return flag;
    }

    private void createTable() {
        String tableName = JOptionPane.showInputDialog(this, "Enter table name:");
        if (tableName != null && !tableName.trim().isEmpty()) {
            if(DB_Access.createTable(tableName)){
            	JOptionPane.showMessageDialog(this, "Table " + tableName + " created.");
        	}
        	else{
        	JOptionPane.showMessageDialog(this, "Table " + "is not created.");
        	}
        }
    }
    
    private boolean openTable() {
        dbTable = JOptionPane.showInputDialog(this, "Enter table name:");
        if (dbTable != null && !dbTable.trim().isEmpty()) {
            if(DB_Access.openTable(dbTable)){
            	DB_Access.tableName = dbTable;
            	JOptionPane.showMessageDialog(this, "Table " + dbTable + " opened.");
            	return true;
          	}
        }
        return false;
    }

    private void insertData() {
    	int id = Integer.parseInt(JOptionPane.showInputDialog(this, "Enter ID:"));
    	String name = JOptionPane.showInputDialog(this, "Enter Name:");
    	int groupNumber = Integer.parseInt(JOptionPane.showInputDialog(this, "Enter Group:"));
    	String coach = JOptionPane.showInputDialog(this, "Enter Coach:");
    	int numberClasses = Integer.parseInt(JOptionPane.showInputDialog(this, "Enter Number of Classes:"));
    	float record = Float.parseFloat(JOptionPane.showInputDialog(this, "Enter Record Time:"));
        if(DB_Access.insertData(id, name, groupNumber, coach, numberClasses, record)){
        	JOptionPane.showMessageDialog(this, "Data inserted.");
        	printData();
        }
        else{
        JOptionPane.showMessageDialog(this, "Data not inserted.");
        }
    }

    private void dropDatabase() {
    int confirm = JOptionPane.showConfirmDialog(this, "Are you sure you want to delete database " + dbName + "?", 
                                                "Confirm", JOptionPane.YES_NO_OPTION);
    if (confirm == JOptionPane.YES_OPTION) {
        DB_Access.dropDatabase(dbName);
        JOptionPane.showMessageDialog(this, "Database deleted.");
    }
}
    private void updateStudent() {
    	int studentId = Integer.parseInt(JOptionPane.showInputDialog("Enter ID of student to update:"));
        String newName = JOptionPane.showInputDialog("Enter new name:");
        int newGroupNumber = Integer.parseInt(JOptionPane.showInputDialog("Enter new group number:"));
        String newCoach = JOptionPane.showInputDialog("Enter new coach:");
        int newNumberClasses = Integer.parseInt(JOptionPane.showInputDialog("Enter new number of classes:"));
        float newRecord = Float.parseFloat(JOptionPane.showInputDialog("Enter new record time:"));
        
        if(DB_Access.updateStudent(studentId, newName, newGroupNumber, newCoach, newNumberClasses, newRecord)){
        	JOptionPane.showMessageDialog(null, "Student updated successfully!");
        	printData();
        }
        else{
        	JOptionPane.showMessageDialog(null, "No student found with this name.");
        }
    }

    private void searchByName() {
        String searchValue = JOptionPane.showInputDialog("Enter student name to search:");
        JOptionPane.showMessageDialog(this, DB_Access.searchByName(searchValue));
    }
    
    private void deleteByName() {
    	String deleteName = JOptionPane.showInputDialog("Enter student name to delete:").trim();
        DB_Access.deleteByName(deleteName);
        JOptionPane.showMessageDialog(this, "Record deleted.");
        printData();
    }
    
	private void clearTable() {
	    if(DB_Access.clearTable()){
	   		JOptionPane.showMessageDialog(this, "Table cleared.");
	    	printData();
	    }
	    else{
	    	JOptionPane.showMessageDialog(this, "Table not cleared.");
	    }
	}
	    
    private void printData(){
    	tableModel.setRowCount(0);
        tableModel.setColumnCount(0);

        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + DB_Access.dbName, DB_Access.userName, DB_Access.userPass);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM " + DB_Access.tableName)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                tableModel.addColumn(metaData.getColumnName(i));
            }

            while (resultSet.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    row[i - 1] = resultSet.getObject(i);
                }
                tableModel.addRow(row);
            }

        } 
        catch (SQLException e) {
            JOptionPane.showMessageDialog(this, "Error when receiving data: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
	}
	
	public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new MainWindow().setVisible(true));
    }
}
