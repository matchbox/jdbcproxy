import java.io.*;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.commons.cli.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

class Connection {
    private java.sql.Connection connection;
    private String driverClass = null;
    private String connectionString = null;
    private String username = null;
    private String password = null;
    
    public Connection(String driverClass, String connectionString, 
            String username, String password){
        this.driverClass = driverClass;
        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
        
        this.connect();
    }

    public void connect(){
        try{
            Class.forName(this.driverClass);
            connection = DriverManager.getConnection(
                    connectionString, username, password);
        }catch(Exception e){
            System.out.println("[ERROR] there was an error when connecting "+
                               "to the data source");
            System.out.println("[ERROR] Exception: " + e);
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    public void printTableNames() throws SQLException{
        DatabaseMetaData md = connection.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
        while(rs.next()){
            System.out.println(rs.getString(3));
        }
    }

    public void execute(String query){
        JSONArray result = new JSONArray();
        try{
            PreparedStatement statement = connection.prepareStatement(query);
            statement.execute();
            ResultSet applications = statement.getResultSet();
            ResultSetMetaData applicationFields = applications.getMetaData();
            while(applications.next()){
                JSONArray row = new JSONArray();
                for(int i=1;i<=applicationFields.getColumnCount();i++){
                    row.add(applications.getObject(i));
                    //System.out.println(applications.getObject(i));
                }
                result.add(row);
            }
        }catch(Exception e){
            System.out.println(e);
        }
        System.out.println(result);
    }
}

public class JDBCProxy {
    public static void main(String[] args) throws java.io.IOException, 
           java.sql.SQLException{
        CommandLine commandLine = null;
        try{
            Options options = new Options();
            options.addOption("u", true, "Username");
            options.addOption("p", true, "Password");
            options.addOption("c", true, "Connection string");
            options.addOption("d", true, "class name for jdbc connection");
            options.addOption("t", false, "print table names and exit");
            options.addOption("help", false, "print this help message");
            CommandLineParser parser = new PosixParser();
            commandLine = parser.parse(options, args);
            if(commandLine.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("jdbcproxy", options);
            }
        }catch( ParseException exp ) {
            System.out.println( "Unexpected exception:" + exp.getMessage() );
            System.exit(1);
        }
        
        Connection conn = new Connection(commandLine.getOptionValue("d"),
                                         commandLine.getOptionValue("c"),
                                         commandLine.getOptionValue("u"),
                                         commandLine.getOptionValue("p"));
        
        if(commandLine.hasOption("t")){
            conn.printTableNames();
            System.exit(0);
        }

        BufferedReader f = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while((line = f.readLine()) != null){
            System.out.println("DEBUG: " + line);
            conn.execute(line);
        }
        System.out.println("ALL DONE");
    }
}

