# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import pyspark

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = (int(params[0]),
               str(params[1]),
               float(params[2]),
               float(params[3]),
               str(params[4]),
               int(params[5]),
               int(params[6])
               )

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(sc, my_dataset_dir):
    
    inputRDD = sc.textFile(my_dataset_dir)
    print(inputRDD.take(5))

    transformedRDD = inputRDD.map(lambda x: process_line(x))
    print(transformedRDD.take(5))

    zeroBikesOnly = transformedRDD.filter(lambda x: x[5] == 0)
    print(zeroBikesOnly.take(5))
    print(zeroBikesOnly.count())

    stationsZeroBikes = zeroBikesOnly.map(lambda x: (x[1], 1))
    print(stationsZeroBikes)

    allTimesZero = stationsZeroBikes.reduceByKey(lambda x, y: x+y)

    sortedByTimes = allTimesZero.sortBy(lambda x: x[1], ascending=False)

    resVal = sortedByTimes.collect()

    print(sortedByTimes.count())
    for item in resVal:
        print(item) 

# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir):
    pass

# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(sc, my_dataset_dir):
    pass

# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(sc, my_dataset_dir, ran_out_times):
    pass

# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(sc, my_dataset_dir):
    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option, ran_out_times):
    # Exercise 1: Number of times each station ran out of bikes (sorted decreasingly by station).
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Average amount of bikes per station and hour window (e.g. [9am, 10am), [10am, 11am), etc. )
    if option == 2:
        ex2(sc, my_dataset_dir)

    # Exercise 3: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the different ran-outs to attend.
    #             Note: n consecutive measurements of a station being ran-out of bikes has to be considered a single ran-out,
    #                   that should have been attended when the ran-out happened in the first time.
    if option == 3:
        ex3(sc, my_dataset_dir)

    # Exercise 4: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the station with biggest number of bikes for each ran-out to be attended.
    if option == 4:
        ex4(sc, my_dataset_dir, ran_out_times)

    # Exercise 5: Total number of bikes that are taken and given back per station (sorted decreasingly by the amount of bikes).
    if option == 5:
        ex5(sc, my_dataset_dir)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 1

    ran_out_times = ['06:03:00', '06:03:00', '08:58:00', '09:28:00', '10:58:00', '12:18:00',
                     '12:43:00', '12:43:00', '13:03:00', '13:53:00', '14:28:00', '14:28:00',
                     '15:48:00', '16:23:00', '16:33:00', '16:38:00', '17:09:00', '17:29:00',
                     '18:24:00', '19:34:00', '20:04:00', '20:14:00', '20:24:00', '20:49:00',
                     '20:59:00', '22:19:00', '22:59:00', '23:14:00', '23:44:00']

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/local/STUDENT-CIT/r00140363/git/big-data-processing"
    my_databricks_path = "/"

    my_dataset_dir = "/Assignment 1/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option, ran_out_times)
