package com.jeffdouglas;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import au.com.bytecode.opencsv.CSVWriter;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class BulkLoader {

  // Salesforce.com credentials
  private String userName = "YOUR_SALESFORCE_USERNAME";
  private String password = "YOUR_SALESFORCE_PASSWORD";
  // the sObject being uploaded
  private String sObject = "Account";
  // the CSV file being uploaded - either manually or from MySQL resultset
  // ie /Users/Me/Desktop/BulkAPI/myfile.csv
  private String csvFileName = "FULL_PATH_TO_CSV_FILE";
  // MySQL connection URL
  String mySqlUrl = "jdbc:mysql://IP_ADDRESS/DATABASE?user=USERNAME&password=PASSWORD";
  // the query that returns results from MySQL
  String mySqlQuery = "SELECT name FROM Account";
  
  private BufferedReader console = null;

  public static void main(String[] args) throws AsyncApiException,
      ConnectionException, IOException {
    BulkLoader example = new BulkLoader();
    example.run();
  }

  /**
   * Allows the user to run multiple operations
   */
  public void run() {
    showMenu();
    console = new BufferedReader(new InputStreamReader(System.in));

    try {
      String choice = console.readLine();
      while ((choice != null) && (Integer.parseInt(choice) != 99)) {
        if (Integer.parseInt(choice) == 1) {
          // Upload a CSV file for import
          runJob(sObject, userName, password, csvFileName);
        } else if (Integer.parseInt(choice) == 2) {
          // Create CSV file from MySQL and upload it for import
          if (createCSVFromMySQL()) {
            System.out.println("Submitting CSV file to Salesforce.com");
            runJob(sObject, userName, password, csvFileName);
          }
        }
        showMenu();
        choice = console.readLine();
      }

    } catch (IOException io) {
      io.printStackTrace();
      System.out.println(io.getMessage());
    } catch (NumberFormatException nf) {
      run();
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }

  }

  /**
   * Displays a menu in the console to choose the operation
   */
  public void showMenu() {
    System.out.println("\n1. Upload from CSV");
    System.out.println("2. Upload from MySQL");
    System.out.println("99. Exit");
    System.out.println(" ");
    System.out.println("---> Operation: ");
  }
  
  /**
   * Converts MySQL resultset to a CSV file
   */
  private boolean createCSVFromMySQL() {
    
    System.out.println("Fetching records from MySQL");
    
    Connection conn = null;
    ResultSet rs = null;
    boolean success = false;
    
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(mySqlUrl);
      
      Statement s = conn.createStatement ();
      s.executeQuery(mySqlQuery);
      rs = s.getResultSet();
      
      // dump the contents to the console
      /**
      while (rs.next ()){
        int idVal = rs.getInt ("id");
        String nameVal = rs.getString ("name");
        System.out.println ("id = "+idVal+", name = "+nameVal);           
      }   
      **/
      
      // write the result set to the CSV file
      if (rs != null) {
        CSVWriter writer = new CSVWriter(new FileWriter(csvFileName), ',');
        writer.writeAll(rs, true);
        writer.close();
        System.out.println("Successfully fetched records from MySQL");
        success = true;
      }
      
    } catch (Exception e) {
      System.err.println("Cannot connect to database server");
      success = false;
    } finally {
      if (rs != null) {
        try {
          rs.close();
          System.out.println("Resultset terminated");
        } catch (Exception e1) { /* ignore close errors */
        }
      }
      if (conn != null) {
        try {
          conn.close();
          System.out.println("Database connection terminated");
        } catch (Exception e2) { /* ignore close errors */
        }
      }

    }
    return success;
    
  }

  /**
   * Creates a Bulk API job and uploads batches for a CSV file.
   */
  public void runJob(String sobjectType, String userName, String password,
      String sampleFileName) throws AsyncApiException, ConnectionException,
      IOException {
    RestConnection connection = getRestConnection(userName, password);
    JobInfo job = createJob(sobjectType, connection);
    List<BatchInfo> batchInfoList = createBatchesFromCSVFile(connection, job,
        sampleFileName);
    closeJob(connection, job.getId());
    awaitCompletion(connection, job, batchInfoList);
    checkResults(connection, job, batchInfoList);
  }

  /**
   * Gets the results of the operation and checks for errors.
   */
  private void checkResults(RestConnection connection, JobInfo job,
      List<BatchInfo> batchInfoList) throws AsyncApiException, IOException {
    // batchInfoList was populated when batches were created and submitted
    for (BatchInfo b : batchInfoList) {
      CSVReader rdr = new CSVReader(connection.getBatchResultStream(
          job.getId(), b.getId()));
      List<String> resultHeader = rdr.nextRecord();
      int resultCols = resultHeader.size();

      List<String> row;
      while ((row = rdr.nextRecord()) != null) {
        Map<String, String> resultInfo = new HashMap<String, String>();
        for (int i = 0; i < resultCols; i++) {
          resultInfo.put(resultHeader.get(i), row.get(i));
        }
        boolean success = Boolean.valueOf(resultInfo.get("Success"));
        boolean created = Boolean.valueOf(resultInfo.get("Created"));
        String id = resultInfo.get("Id");
        String error = resultInfo.get("Error");
        if (success && created) {
          System.out.println("Created row with id " + id);
        } else if (!success) {
          System.out.println("Failed with error: " + error);
        }
      }
    }
  }

  /**
   * Closes the job
   */
  private void closeJob(RestConnection connection, String jobId)
      throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setId(jobId);
    job.setState(JobStateEnum.Closed);
    connection.updateJob(job);
  }

  /**
   * Wait for a job to complete by polling the Bulk API.
   */
  private void awaitCompletion(RestConnection connection, JobInfo job,
      List<BatchInfo> batchInfoList) throws AsyncApiException {
    long sleepTime = 0L;
    Set<String> incomplete = new HashSet<String>();
    for (BatchInfo bi : batchInfoList) {
      incomplete.add(bi.getId());
    }
    while (!incomplete.isEmpty()) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
      }
      System.out.println("Awaiting results..." + incomplete.size());
      sleepTime = 10000L;
      BatchInfo[] statusList = connection.getBatchInfoList(job.getId())
          .getBatchInfo();
      for (BatchInfo b : statusList) {
        if (b.getState() == BatchStateEnum.Completed
            || b.getState() == BatchStateEnum.Failed) {
          if (incomplete.remove(b.getId())) {
            System.out.println("BATCH STATUS:\n" + b);
          }
        }
      }
    }
  }

  /**
   * Create a new job using the Bulk API.
   */
  private JobInfo createJob(String sobjectType, RestConnection connection)
      throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation(OperationEnum.insert);
    job.setContentType(ContentType.CSV);
    job = connection.createJob(job);
    System.out.println(job);
    return job;
  }

  /**
   * Create the RestConnection used to call Bulk API operations.
   */
  private RestConnection getRestConnection(String userName, String password)
      throws ConnectionException, AsyncApiException {
    ConnectorConfig partnerConfig = new ConnectorConfig();
    partnerConfig.setUsername(userName);
    partnerConfig.setPassword(password);
    partnerConfig
        .setAuthEndpoint("https://www.salesforce.com/services/Soap/u/17.0");
    // Creating the connection automatically handles login and stores
    // the session in partnerConfig
    new PartnerConnection(partnerConfig);
    // When PartnerConnection is instantiated, a login is implicitly
    // executed and, if successful,
    // a valid session is stored in the ConnectorConfig instance.
    // Use this key to initialize a RestConnection:
    ConnectorConfig config = new ConnectorConfig();
    config.setSessionId(partnerConfig.getSessionId());
    // The endpoint for the Bulk API service is the same as for the normal
    // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
    String soapEndpoint = partnerConfig.getServiceEndpoint();
    String apiVersion = "17.0";
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint
        .indexOf("Soap/"))
        + "async/" + apiVersion;
    config.setRestEndpoint(restEndpoint);
    // This should only be false when doing debugging.
    config.setCompression(true);
    // Set this to true to see HTTP requests and responses on stdout
    config.setTraceMessage(false);
    RestConnection connection = new RestConnection(config);
    return connection;
  }

  /**
   * Create and upload batches using a CSV file. The file into the appropriate
   * size batch files.
   */
  private List<BatchInfo> createBatchesFromCSVFile(RestConnection connection,
      JobInfo jobInfo, String csvFileName) throws IOException,
      AsyncApiException {
    List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
    BufferedReader rdr = new BufferedReader(new InputStreamReader(
        new FileInputStream(csvFileName)));
    // read the CSV header row
    byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
    int headerBytesLength = headerBytes.length;
    File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");

    // Split the CSV file into multiple batches
    try {
      FileOutputStream tmpOut = new FileOutputStream(tmpFile);
      int maxBytesPerBatch = 10000000; // 10 million bytes per batch
      int maxRowsPerBatch = 10000; // 10 thousand rows per batch
      int currentBytes = 0;
      int currentLines = 0;
      String nextLine;
      while ((nextLine = rdr.readLine()) != null) {
        byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
        // Create a new batch when our batch size limit is reached
        if (currentBytes + bytes.length > maxBytesPerBatch
            || currentLines > maxRowsPerBatch) {
          createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
          currentBytes = 0;
          currentLines = 0;
        }
        if (currentBytes == 0) {
          tmpOut = new FileOutputStream(tmpFile);
          tmpOut.write(headerBytes);
          currentBytes = headerBytesLength;
          currentLines = 1;
        }
        tmpOut.write(bytes);
        currentBytes += bytes.length;
        currentLines++;
      }
      // Finished processing all rows
      // Create a final batch for any remaining data
      if (currentLines > 1) {
        createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
      }
    } finally {
      tmpFile.delete();
    }
    return batchInfos;
  }

  /**
   * Create a batch by uploading the contents of the file. This closes the
   * output stream.
   */
  private void createBatch(FileOutputStream tmpOut, File tmpFile,
      List<BatchInfo> batchInfos, RestConnection connection, JobInfo jobInfo)
      throws IOException, AsyncApiException {
    tmpOut.flush();
    tmpOut.close();
    FileInputStream tmpInputStream = new FileInputStream(tmpFile);
    try {
      BatchInfo batchInfo = connection.createBatchFromStream(jobInfo,
          tmpInputStream);
      System.out.println(batchInfo);
      batchInfos.add(batchInfo);

    } finally {
      tmpInputStream.close();
    }
  }
}
