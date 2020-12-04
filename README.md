# cs166_final_project

The objective of this phase is to develop a client application of the airline model using Java Database Connecter which 
supports queries such as booking flights, adding details of a flight into the database, retrieving details of a flight, etc.
This project was written by Devang Aggarwal and Justin Luo.

Devang wrote the functions for: AddPlane, AddPilot, AddFlight, AddTechnician, and BookFlight

Justin wrote the functions for: ListsTotalNumberOfRepairsPerPlane, ListTotalNumberOfRepairsPerYear, FindPassengersCountWithStatus

# How to run

1. cd Postgresql
2. source ./startPostgresql.sh
3. source ./createPostgreDB.sh
4. cd ..
5. cd java
6. mkdir bin
7. source ./compile.sh
8. source ./run.sh $username_DB PORTNUMBER $username

# After you are done:
1. cd ..
2. cd Postgresql
3. source ./stopPostgreDB





