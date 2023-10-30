COEN 6313
Assignment 2

### Steps to run the files
1. Unzip and extract the file to the system
2. There are 3 files viz. data, insertion.py, main.py
3. Change the MongoDB url in the 'insertion.py' and 'main.py' files
4. Run 'python insertion.py' file for all the data files
5. Run 'python main.py' file
6. Enter the values as asked.
7. You should get a response


### Explaining the code
### Data folder
1. The data file contains the test data

### The 'insertion.py' file
1. This file will insert the data from the 'data' folder to 'MongoDB'
2. Change the MongoDB url in oder to connect MongoDB to the local machine
3. Then sequentially add all the data files from the 'data' folder by commenting and uncommenting accordingly
4. Therfore, all the csv files will be read and saved as 'Collections' in MongoDB and all data as 'Documents' respectively

### The 'main.py' file
1. This file will run aggregation pipeline in MongoDB and give us the desired results
2. Change the MongoDB url in oder to connect MongoDB to the local machine
3. Enter the values as asked and the ouput gives the desired analytics result for every batch

