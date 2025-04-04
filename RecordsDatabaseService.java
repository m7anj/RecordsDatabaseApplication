package fsad.a3.a3;
/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2633724
 *
 */

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.


public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2];
    private ResultSet outcome   = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){
        //TO BE COMPLETED
        this.serviceSocket = aSocket;
        start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr = new String[2];
        this.requestStr[0] = "";
        this.requestStr[1] = "";

		String tmp = "";

        try {
            InputStreamReader inputStreamReader = new InputStreamReader(serviceSocket.getInputStream());
            StringBuilder reqBuilder = new StringBuilder();
            char a;


            while ((a = (char) inputStreamReader.read()) != '#') {
                reqBuilder.append((char) a);
            }


            String req = reqBuilder.toString().trim();

            if (!req.isEmpty()) {
                String terminatedRequest = req.replace("#", "");
                String[] sqlparams = terminatedRequest.split(";");

                this.requestStr[0] = sqlparams[0];
                this.requestStr[1] = sqlparams[1];
            }

        } catch(IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }


        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		this.outcome = null;

        String sql = "SELECT record.title, record.label, record.genre, record.rrp, COUNT(recordcopy.recordID) AS num_copies " +
                "FROM record " +
                "INNER JOIN artist ON artist.artistID = record.artistID " +
                "INNER JOIN recordcopy ON recordcopy.recordID = record.recordID " +
                "INNER JOIN recordshop ON recordshop.recordshopID = recordcopy.recordshopID " +
                "WHERE artist.lastname = ? AND recordshop.city = ? " +
                "GROUP BY record.title, record.label, record.genre, record.rrp";
        //Connect to the database
        //TO BE COMPLETED
		try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);){

            //Make the query
            //TO BE COMPLETED
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

                //Process query
                //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
                preparedStatement.setString(1, this.requestStr[0]);
                preparedStatement.setString(2, this.requestStr[1]);


                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    RowSetFactory rowSetFactory = RowSetProvider.newFactory();
                    //Clean up
                    //TO BE COMPLETED
                    CachedRowSet cachedRowSet = rowSetFactory.createCachedRowSet();
                    cachedRowSet.populate(resultSet);
                    this.outcome = cachedRowSet;
                }
            }
		} catch (Exception e) {
            System.out.println(e);
        }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
			//Return outcome
			//TO BE COMPLETED
            OutputStream outcomeStream = this.serviceSocket.getOutputStream();
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(outcomeStream);
            outcomeStreamWriter.writeObject(this.outcome);
            outcomeStreamWriter.flush();

            while (this.outcome.next()) {
                System.out.println(
                        this.outcome.getString("title") + " | " +
                        this.outcome.getString("label") + " | " +
                        this.outcome.getString("genre") + " | " +
                        this.outcome.getString("rrp") + " | " +
                        this.outcome.getString("num_copies"));
            }

            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

            //Terminating connection of the service socket
			//TO BE COMPLETED
            this.serviceSocket.close();
			
        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();

            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
