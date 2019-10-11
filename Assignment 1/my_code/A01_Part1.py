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
        res = tuple(params)

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(sc, my_dataset_dir):
    pass

    # 1. We load the dataset into an inputRDD


    # 2. We count the total amount of entries


    # 3. We print the result



# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir):
    pass

    # 1. We load the dataset into an inputRDD


    # 2. We process each line to get the relevant info


    # 3. We project just the info we are interested into


    # 4. We get just the entries that are different


    # 5. We count such these entries


    # 6. We print the result



# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(sc, my_dataset_dir):
    pass

    # 1. We load the dataset into an inputRDD


    # 2. We process each line to get the relevant info


    # 3. We project just the info we are interested into


    # 4. We get just the entries that are different


    # 5. We collect such these entries


    # 6. We print them



# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(sc, my_dataset_dir):
    pass

    # 1. We load the dataset into an inputRDD


    # 2. We process each line to get the relevant info


    # 3. We project just the info we are interested into


    # 4. We get just the entries that are different


    # 5. We sort them by their longitude


    # 6. We collect such these entries


    # 7. We print them


# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(sc, my_dataset_dir):
    pass

    # 1. We load the dataset into an inputRDD


    # 2. We process each line to get the relevant info


    # 3. We filter the bikes of "Kent Station"


    # 4. We project just the info we are interested into


    # 5. We compute the average amount of bikes


    # 6. We print this info by the screen


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option):
    # Exercise 1: Total amount of entries in the dataset.
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Number of Coca-cola bikes stations in Cork.
    if option == 2:
        ex2(sc, my_dataset_dir)

    # Exercise 3: List of Coca-Cola bike stations.
    if option == 3:
        ex3(sc, my_dataset_dir)

    # Exercise 4: Sort the bike stations by their longitude (East to West).
    if option == 4:
        ex4(sc, my_dataset_dir)

    # Exercise 5: Average number of bikes available at Kent Station.
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

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/7_Assignments/A01/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option)
