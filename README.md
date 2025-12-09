## Intermediate Data Engineer test
### Context
A supermarket stores its transaction data in a relational database for further analysis. The transaction data is uploaded to the system as a CSV file multiple times per day. Each transaction has a unique `id`. As a data engineer, your task is to implement and maintain a pipeline to store the transaction data in the database.

### Elements
For the test, the following elements are provided:
- A sqlite database contains historical transactions data
- A csv file `retail_15_01_2022.csv` contains transaction data of 15/01/2022.
- The tax for all products is 20%.

### Requirements
#### The implementation of the ETL workflow
For that , I created a new project structure consisting of having a folder for etl modules in src folder, each file contains the functions of each step , from extracting , transforming and loading data to the final destination (retail.db) in our case. 

1- Extract step :

As first analysis , I noticed that the csv file doesnt have a date column . But we can extract it from the name of each csv file. Assuming that csv files that will arrive will have this structure in their name , we can always extract the date of the day from the file name and then we can add it later in the transofrmation part.The rest is to just extarct tha data as dataframe. I choose to use sqlite3 and pandas as I'm famailiar with these tools but we can imaging using sqlalchemy as well.

2- Transform step :

This step consists of adding the date column as said, renaming also the description that represents the name of products and also reordering the columns in order to match the same order as the target table in the database. 

We can also imagine other kind of transformations for this use cas , like type validation for numeric columns for example , verifiying the tax calculation , removing duplicates etc.

3- Load step : 

This step consists of verifiying first that we dont have some duplicated rows by comparing the unique id of our datafarme (csv file) and the target table ind the database . After that we can insert the new rows to the datbase.


In the src directory , we have run_etl.py file that is responsible to run our ETL pipeline. It takes in the arguments the path of the csv file and the path of database. 
In my project structure here, I created a data foler where we can find a database foler containing the retail database and onther folder named raw where we can find csv files.




As implementation is only half of the work for a Data Engineer, you're also asked to implement test cases to verify things work as expected. An example of test is provided in the file `test.py`. You need to complete it. Do not hesitate to add more relevant test cases.

#### Explore the data using SQL
For that , i created in src the queries where we can find .sql file for each question's query . These queries will be called in a .py file in order to run and see the results. 



#### Deployment (optional)
Of course, the workflow cannot run on the developer's machine, we need to deploy it and automate the process. Can you list the necessary elements of such a system ?

