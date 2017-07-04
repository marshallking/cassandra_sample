package cassandra_sample;
 
import static java.lang.System.out;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.AlreadyExistsException;;


public class cassandra
{
   
	public static void main(final String[] args)throws Exception
	{
	
	   final cassandra client = new cassandra();
	   final String ipAddress = args.length > 0 ? args[0] : "127.0.0.1";
	   final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
	   out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
	   
	   /* Simple steps to create a keyspace,create table in keyspace,Insert data to table,and finally query (Select) that data.*/
	   client.connect(ipAddress, port);  
	   client.CreateKeyspace(); 
	   client.CreateTable();
	   client.CreateIndex();
	   client.InsertData();
	   client.UpdateData();
	   client.ReadData();
	   client.close();
	
	}
	
   /** Cassandra Cluster. */
   private Cluster cluster;
   /** Cassandra Session. */
   private Session session;
   
   
   /**
    * Connect to Cassandra Cluster specified by provided node IP
    * address and port number.
    *
    * @param node Cluster node IP address.
    * @param port Port of cluster host.
    */

   public void connect(final String node, final int port)
   {  
      this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
      final Metadata metadata = cluster.getMetadata();
      out.printf("Connected to cluster: %s\n", metadata.getClusterName());
      for (final Host host : metadata.getAllHosts())
      {
         out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
         host.getDatacenter(), host.getAddress(), host.getRack());
      }
      session = cluster.connect();
   }
   /**
    * Provide my Session.
    *
    * @return My session.
    */
   
   public void CreateKeyspace()
   {
	      //Query
	      String query = "CREATE KEYSPACE ShannonDemo WITH replication "
	         + "= {'class':'SimpleStrategy', 'replication_factor':1};";
	      
	      /*-----------------------------------------------------------
	       *  or you can do this  -- with the  'if not exist'
	       * 
	       * no exception will be throw if ya do this commented code below
	       * 
	       * 
	      String query = "CREATE KEYSPACE if not exists ShannonDemo WITH replication "
	 	         + "= {'class':'SimpleStrategy', 'replication_factor':1};";
	      --------------------------------------------------------------*/
	      try {
				session.execute(query);
				System.out.println("Keyspace shannondemo created."); 
		  } catch (AlreadyExistsException e) {
				System.out.println(e.getMessage());
		  }
	     
	      //using the KeySpace
	      session.execute("USE ShannonDemo");
	     
   }
   
   public void CreateTable()
   {
	     //Query
	      String query = "CREATE TABLE testTable(emp_id int PRIMARY KEY, "
	         + "emp_name text, "
	         + "emp_city text, "
	         + "emp_sal varint, "
	         + "emp_phone varint );";
	      
	      session.execute("USE ShannonDemo");              
	      //Executing the query
	          
	      try {
				session.execute(query);
				System.out.println("Table testTable  created"); 
		  } catch (AlreadyExistsException e) {
				System.out.println(e.getMessage());
		  }    
   }
   
   public void CreateIndex()
   {
	   String query = "CREATE INDEX if not exists name ON testTable (emp_name);";
	   
	   session.execute("USE ShannonDemo");      
	
	   //Executing the query
	   session.execute(query);
	   System.out.println("Index created on emp_name");
   }

   public void InsertData()
   {
	        String query1 = "INSERT INTO testTable (emp_id, emp_name, emp_city, emp_phone, emp_sal)" +
                "VALUES(1,'robin', 'Hyderabad', 9848022339, 40000);";
			 
			String query2 = "INSERT INTO testTable (emp_id, emp_name, emp_city, emp_phone, emp_sal)" +
			    "VALUES(2,'bill', 'Hyderabad', 9848022339, 43000);";
			 
			String query3 = "INSERT INTO testTable (emp_id, emp_name, emp_city, emp_phone, emp_sal)" +
			    "VALUES(3,'rahman', 'Chennai', 9848022330, 45000);";
			   
			session.execute("USE ShannonDemo"); 
			
			session.execute(query1); 
			session.execute(query2);
			session.execute(query3); 
	       
	      //using the KeySpace
	      
	      System.out.println("Data inserted into testTable"); 
   }
   
   public void UpdateData()
   {
	   //query
	   String query = " UPDATE testTable SET emp_city='Delhi',emp_sal=50000 WHERE emp_id = 2;" ;    
	   session.execute("USE ShannonDemo");     
	   //Executing the query
	   session.execute(query);
	   System.out.println("Data updated");
   }   
   public void ReadData() 
   {
	       //queries
	        String query = "SELECT * FROM testTable";
			   
			session.execute("USE ShannonDemo"); 
				 
			//Getting the ResultSet
			ResultSet result = session.execute(query);
			System.out.println("All Data read from testtable\n");  
		    System.out.println(result.all()); 
		     
		    query = "SELECT * FROM testTable WHERE emp_name = 'robin'";
		    result = session.execute(query);
		    System.out.println(result.all()); 
   }
  
   public Session getSession()
   {
      return this.session;
   }
   /** Close cluster. */
   public void close()
   {
      cluster.close();
   }	
}
 

 