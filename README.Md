## BCG GAMMA Case Study:

### Analytics
Application should perform below analysis and store the results for each analysis.
* Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
* Analysis 2: How many two-wheelers are booked for crashes? 
* Analysis 3: Which state has the highest number of accidents in which females are involved? 
* Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
* Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
* Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
* Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
* Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

### Expected Output:
1. Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2. Code should be properly organized in folders as a project.
3. Input data sources and output should be config driven
4. Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)
5. Share the entire project as zip or link to project in GitHub repo.

For this case study I took inspiration from [Best Practices Writing Production-Grade PySpark Jobs](https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f); the job structure looks like this:

```.
├── Data
│   └── Input
│   └── Output    
├── Makefile
├── README.Md
├── dist
│   ├── config.ini
│   ├── main.py
│   └── utils.zip
├── requirements.txt
└── src
    ├── config.ini
    ├── main.py
    └── utils
        ├── Accident.py
        ├── __init__.py
        └── helpers.py
```    

## Running the ETL job
1. Go to the Project Directory: `$cd BCG_Case_Study`
2. On terminal, run `$make build`. This will build the project to run via spark-submit. In this process a new folder with 
   name "dist" is created.
3. Execute spark submit with this command `$cd dist && spark-submit --py-files utils.zip  main.py
`