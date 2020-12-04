/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddPlane(DBproject esql) {//1
        int id;
	String make;
	String model;
	int age;
	int seats;
		while (true)
		{
			System.out.print("Please input the plane ID number: ");
			try 
			{
				id = Integer.parseInt(in.readLine()); //user input
				break;
			}
			catch (Exception e) 
			{
				System.out.println("The input you entered is invalid. Please try again!");
				continue;
			}
		}	
		while (true)
		{
			System.out.print("Please input the Plane make: ");
			try 
			{
				make = in.readLine(); //user input
				if(make.length() < 1 || make.length() >= 33) 
				{
					throw new RuntimeException("The Make you have entered is invalid.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		while (true)
		{
			System.out.print("Please input the Plane model: ");
			try 
			{
				model = in.readLine(); //user input
				if(model.length() < 1 || model.length() >= 65) 
				{
					throw new RuntimeException("The model you entered is invalid.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		
		while (true)
		{
			System.out.print("Please input the plane age: ");
			try 
			{
				age = Integer.parseInt(in.readLine()); //user input
				if(age < 0) 
				{
					throw new RuntimeException("Please enter a valid Plane age.");
				}
				break;
			}
			catch (NumberFormatException e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}		
		while (true)
		{
			System.out.print("Input Number of Plane Seats: ");
			try 
			{
				seats = Integer.parseInt(in.readLine()); //user input
				if(seats <= 0 || seats > 499) 
				{
					throw new RuntimeException("You have entered an invalid amount of seats. Please try again.");
				}
				break;
			}
			catch (NumberFormatException e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
			catch (Exception e)
			{
				System.out.println(e);
				continue;
			}
		}
		try 
		{
			String query = "INSERT INTO Plane (id, make, model, age, seats) VALUES (" + id + ", \'" + make + "\', \'" + model + "\', " + age + ", " + seats + ");";
			esql.executeUpdate(query);
		}
		catch (Exception e) 
		{
			System.err.println (e.getMessage());
		}
	}

	public static void AddPilot(DBproject esql) {//2
        int id;
	String fullname;
	String nationality;
		while (true)
		{
			System.out.print("Please input the Pilot ID: ");
			try {
				id = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}
		}	
		while (true)
		{
			System.out.print("Please input the full name of the pilot: ");
			try 
			{
				fullname = in.readLine();
				if(fullname.length() <= 0 || fullname.length() >= 129) 
				{
					throw new RuntimeException("Please enter a valid pilot name.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		while (true)
		{
			System.out.print("Input Pilot Nationality: ");
			try 
			{
				nationality = in.readLine();
				if(nationality.length() <= 0 || nationality.length() >= 25) 
				{
					throw new RuntimeException("Please enter a valid Pilot nationality.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		
		try 
		{
			String query = "INSERT INTO Pilot (id, fullname, nationality) VALUES (" + id + ", \'" + fullname + "\', \'" + nationality + "\');";
			esql.executeUpdate(query);
		}
		catch (Exception e)
		{
			System.err.println (e.getMessage());
		}
	}

	public static void AddFlight(DBproject esql) {//3
		// Given a pilot, plane and flight, adds a flight in the DB
        int fnum;
	int cost;
	int num_sold;
	int num_stops;
	LocalDate departureDate;
	LocalDate arrivalDate;
	String actual_departure_date;
	String actual_arrival_date;
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
	String arrival_airport;
	String departure_airport;
		while (true)
		{
			System.out.print("Please input flight number: ");
			try 
			{
				fnum = Integer.parseInt(in.readLine()); //user input
				break;
			}
			catch (Exception e)
			{
				System.out.println("Your input is invalid!");
				continue;
			}
		}
		
		while (true)
		{
			System.out.print("Please input flight cost: ");
			try 
			{
				cost = Integer.parseInt(in.readLine());
				if(cost < 1)
				{
					throw new RuntimeException("Invalid Flight cost.");
				}
				break;
			}
			catch (NumberFormatException e) 
			{
				System.out.println("Your input is invalid!");
				continue;
			}catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}	
		while (true)
		{
			System.out.print("Please input number of seats sold: ");
			try 
			{
				num_sold = Integer.parseInt(in.readLine());
				if(num_sold <= -1) 
				{
					throw new RuntimeException("Please enter valid number of seats sold.");
				}
				break;
			}
			catch (NumberFormatException e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		while (true){
			System.out.print("Input Number of Stops: ");
			try 
			{
				num_stops = Integer.parseInt(in.readLine());
				if(num_stops < 1) 
				{
					throw new RuntimeException("Please enter valid number of stops.");
				}
				break;
			}
			catch (NumberFormatException e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		while (true)
		{
			System.out.print("Please input Departure Time in the following format (YYYY-MM-DD hh:mm): ");
			try 
			{
				actual_departure_date = in.readLine();
				departureDate = LocalDate.parse(actual_departure_date, formatter);
				break;
			}
			catch (Exception e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
		}
		
		while (true)
		{
			System.out.print("Please input Arrival Time in the following format (YYYY-MM-DD hh:mm): ");
			try 
			{
				actual_arrival_date = in.readLine();
				arrivalDate = LocalDate.parse(actual_arrival_date, formatter);
				if(arrivalDate.isAfter(departureDate) == false) 
				{
					throw new RuntimeException();
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
		}
		while (true)
		{
			System.out.print("Please input Arrival Airport: ");
			try 
			{
				arrival_airport = in.readLine();
				if(arrival_airport.length() < 1 || arrival_airport.length() >= 6)
				{
					throw new RuntimeException("Please enter a valid Arrival Airport.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		
		while (true)
		{
			System.out.print("Please input Departure Airport: ");
			try {
				departure_airport = in.readLine();
				if(departure_airport.length() < 1 || departure_airport.length() >= 6) 
				{
					throw new RuntimeException("Please enter a valid Departure Airport.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		
		try 
		{
			String query = "INSERT INTO Flight (fnum, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_airport, departure_airport) VALUES (" + fnum + ", " + cost + ", " + num_sold + ", " + num_stops + ", \'" + actual_departure_date + "\', \'" + actual_arrival_date + "\', \'" + arrival_airport + "\', \'" + departure_airport + "\');";
			esql.executeUpdate(query);
		}
		catch (Exception e) 
		{
			System.err.println (e.getMessage());
		}
	}

	public static void AddTechnician(DBproject esql) {//4
        int id;
	String full_name;
		while (true)
		{
			System.out.print("Please input Technician ID: ");
			try 
			{
				id = Integer.parseInt(in.readLine());
				break;
			}
			catch (Exception e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
		}
		while (true)
		{
			System.out.print("Input Technician Name: ");
			try 
			{
				full_name = in.readLine();
				if(full_name.length() < 1 || full_name.length() >= 129) 
				{
					throw new RuntimeException("Please enter a valid Technician name.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}
		try 
		{
			String query = "INSERT INTO Technician (id, full_name) VALUES (" + id + ", \'" + full_name + "\');";
			esql.executeUpdate(query);
		}
		catch (Exception e) 
		{
			System.err.println (e.getMessage());
		}
	}

	public static void BookFlight(DBproject esql) {//5
		//Given a customer and a flight that he/she wants to book, add a reservation to the DB
        int cid;
	int fid;
		while (true)
		{
			System.out.print("Please input Customer ID: ");
			try 
			{
				cid = Integer.parseInt(in.readLine());
				break;
			}
			catch (Exception e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
		}	
		while (true)
		{
			System.out.print("Please input flight number: ");
			try 
			{
				fid = Integer.parseInt(in.readLine());
				break;
			}
			catch (Exception e) 
			{
				System.out.println("Your input is invalid!");
				continue;
			}
		}

		try 
		{
			String query = "SELECT status\nFROM Reservation\nWHERE cid = " + cid + " AND fid = " + fid + ";";
			String booking;
			if(esql.executeQueryAndPrintResult(query) == 0) 
			{
				while(true) 
				{
					System.out.println("This reservation does not exist. Would you like to book a new reservation? (y/n)");
					try 
					{
						booking = in.readLine();
						if(booking.equals("y") == true) 
						{
							int rnum;
							String status;
							while(true)
							{
								System.out.print("Please input New Reservation Number: ");
								try 
								{
									rnum = Integer.parseInt(in.readLine());
									break;
								}
								catch (Exception e) 
								{
									System.out.println("Your input is invalid.");
									continue;
								}
							}
							while(true)
							{
								System.out.print("Please input New Reservation Status: ");
								try {
									status = in.readLine();
									if(status.equals("W") == false && status.equals("R") == false && status.equals("C") == false) 
									{
										throw new RuntimeException("Please enter a valid status.");
									}
									break;
								}
								catch (Exception e) 
								{
									System.out.println(e);
									continue;
								}
							}
							try 
							{
								query = "INSERT INTO Reservation (rnum, cid, fid, status) VALUES (" + rnum + ", " + cid + ", " + fid + ", \'" + status + "\');";
								esql.executeUpdate(query);
							}catch (Exception e) 
							{
								System.err.println (e.getMessage());
							}
						}
						else if(booking.equals("n") == false) 
						{
							throw new RuntimeException("Your input is invalid.");
						}
						break;
					}
					catch (Exception e) 
					{
						System.out.println(e);
						continue;
					}
				}
			}
			else 
			{
				while(true)
				{
					try
					{
						System.out.println("Do you want to update the reservation? (y/n)");
						booking = in.readLine();
						if(booking.equals("y") == true) 
						{
							String status;
							while(true)
							{
								System.out.print("Please Update Reservation Status: ");
								try 
								{
									status = in.readLine();
									if(status.equals("W") == false && status.equals("R") == false && status.equals("C") == false)
									{
										throw new RuntimeException("Please enter a valid status.");
									}
									break;
								}
								catch (Exception e) 
								{
									System.out.println(e);
									continue;
								}
							}
							try 
							{
								query = "UPDATE Reservation SET status = \'" + status + "\' WHERE cid = " + cid + " AND fid = " + fid + ";";
								esql.executeUpdate(query);
							}
							catch (Exception e) 
							{
								System.err.println (e.getMessage());
							}
						}
						else if(booking.equals("n") == false) 
						{
							throw new RuntimeException("Your input is invalid.");
						}
						break;
					}
					catch (Exception e) 
					{
						System.out.println(e);
						continue;
					}
				}
			}
		}
		catch (Exception e) 
		{
			System.err.println (e.getMessage());
		}
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
        int fnum;
	String actual_departure_date;
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
		while(true)
		{
			System.out.print("Please input flight number: ");
			try 
			{
				fnum = Integer.parseInt(in.readLine());
				break;
			}
			catch (Exception e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
		}
		
		while(true)
		{
			System.out.print("Please input departure time in the format of (YYYY-MM-DD hh:mm): ");
			try 
			{
				actual_departure_date = in.readLine();
				LocalDate departDate = LocalDate.parse(actual_departure_date, formatter);
				break;
			}
			catch (Exception e) 
			{
				System.out.println("Your input is invalid.");
				continue;
			}
		}

		try 
		{
			String query = "SELECT Total_Seats - Seats_Sold as \"Seats Available\"\nFROM(\nSELECT P.seats as Total_Seats\nFROM Plane P, FlightInfo FI\nWHERE FI.flight_id = " + fnum + " AND FI.plane_id = P.id\n)total,\n(\nSELECT F.num_sold as Seats_Sold\nFROM Flight F\nWHERE F.fnum = " + fnum + " AND F.actual_departure_date = \'" + actual_departure_date + "\'\n)sold;";
			
			if(esql.executeQueryAndPrintResult(query) == 0) 
			{
				System.out.println("Does not exist.");
			}
		}
		catch (Exception e) 
		{
			System.err.println (e.getMessage());
		}
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
		try {
			String query = "SELECT P.id, count(R.rid)\nFROM Plane P, Repairs R\nWHERE P.id = R.plane_id\nGROUP BY P.id\nORDER BY count DESC;";
			
			esql.executeQueryAndPrintResult(query);
		}catch (Exception e) {
			System.err.println (e.getMessage());
		}
	}

	//public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
	//}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
        try {
			String query = "SELECT EXTRACT (year FROM R.repair_date) as \"Year\", count(R.rid)\nFROM repairs R\nGROUP BY \"Year\"\nORDER BY count ASC;";
			esql.executeQueryAndPrintResult(query);
		}
	catch (Exception e) 
 		{
			System.err.println (e.getMessage());
		}
	}
	
	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		//Find how many passengers there are with a status (i.e. W,C,R) and list that number.
        int fnum;
	String status;
		while(true){
			System.out.print("Please input flight number: ");
			try 
			{
				fnum = Integer.parseInt(in.readLine());
				break;
			}
			catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}
		}
		
		while(true)
		{
			System.out.print("Please input Status: ");
			try 
			{
				status = in.readLine();
				if(status.equals("W") == false && status.equals("R") == false && status.equals("C") == false)
				{
					throw new RuntimeException("Invalid status.");
				}
				break;
			}
			catch (Exception e) 
			{
				System.out.println(e);
				continue;
			}
		}

		try 
		{
			String query = "SELECT COUNT(*)\nFROM Reservation\nWHERE fid = " + fnum + " AND status = \'" + status + "\';";
			esql.executeQueryAndPrintResult(query);
		}
		catch (Exception e) 
		{
			System.err.println (e.getMessage());
		}
	}
	}

