import argparse
import fnmatch
import os
import sys
import json
import time
import pandas as pd 
from pandas.io.json import json_normalize
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join


startTime = time.time()
parser = argparse.ArgumentParser()

#Input Path argument
parser.add_argument("inputPath" , help="Enter directory path to listen on it")

#Target Path argument
parser.add_argument("targetPath" , help="Enter directory path to listen on it")

#print(inputDirectory)
#print(type(inputDirectory))

parser.add_argument("-u", "--timestamp", action="store_true", dest="timestamp", default=False,
                    help="Change timestamp to be readable")


args = parser.parse_args()

inputDirectory = args.inputPath

TargetDirectory = args.targetPath

#print("Input Path is :   " +inputDirectory)


files = [item for item in listdir(inputDirectory) if isfile(join(inputDirectory, item)) if fnmatch.fnmatch(item, '*.json') if not fnmatch.fnmatch(item, 'Loaded_*.json') ]
print(files)
#print(files)
#print(type(files))
checksums = {}

duplicates = []

for filename in files:
   
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
       
        checksum = proc.stdout.read().split()[0]
        
        if checksum in checksums:
            duplicates.append(filename)
        checksums[checksum] = filename
        
print(f"Found Duplicates: {duplicates}")

for rm in duplicates:
    os.remove(inputDirectory+'/'+rm)
    print(rm + " this file Deleted")

uniqueFiles = list(set(files) - set(duplicates))
print(uniqueFiles)

for loadFiles in uniqueFiles:

    
    records = [json.loads(line) for line in open(inputDirectory +'/'+loadFiles)]
    # Convert Json list to DataFrame
    df = json_normalize(records)
    # Select needed columns only
    df = df[['a' , 'tz' ,'r' ,'u' ,'t' ,'hc' ,'cy' ,'ll']]
    #print(df)
    # Drop Null Values
    df = df.dropna(axis=0)
    # Split ll into to columns longitude & latitude
    df[['longitude','latitude']] = pd.DataFrame(df.ll.tolist() , index= df.index)
    # Drop column ll from DataFrame
    df = df.drop(['ll'] , axis=1)
    # Split Column a into two columns web_browser & operating_sys by  split data between parenthesis
    df['web_browser'] = df["a"].str.split('(').str[0]
    df['operating_sys'] = df["a"].str.split('(').str[1]
    df['operating_sys'] = df["operating_sys"].str.split(' ').str[0]
    df['operating_sys'] = df["operating_sys"].str.split(';').str[0]
    # Drop column a
    df.drop(['a'] , axis=1)
    # Rename columns name as required 
    df = df.rename(columns={'tz': 'time_zone', 'r': 'from_url', 'cy': 'city', 't': 'time_in', 'hc': 'time_out' ,  'u': 'to_url'})              
    # ordering colums as required
    df = df[['web_browser' , 'operating_sys', 'from_url' , 'to_url','city' ,'longitude','latitude','time_zone' ,'time_in','time_out']]
    # Split URL
    df ['from_url'] = df['from_url'].str.split("/" , 3).str[2]
    # Fillna that removed due to split and back it again
    df[['from_url']] = df[['from_url']].fillna('direct')
    
    # Using optional arguments to convert unix timestamp to readable date
    if not args.timestamp:
        #overwrite time_in with date format
        df['time_in'] = pd.to_datetime(df.time_in, unit='s')
        #overwrite time_out with date format
        df['time_out'] = pd.to_datetime(df.time_out, unit='s')
        
        # Drop Null Values
        df = df.dropna(axis=0)
        
        n_rows = df.shape[0]
        print("Number of row of file"+ loadFiles + " :  " + str(n_rows))
        # To remove .json from file to convert it to csv after that
        csvFileName = loadFiles.rsplit('.' , 1)
        #print(csvFileName[0])
        # Load DataFrame to CSV file
        df.to_csv (TargetDirectory +'Done_' + csvFileName[0] + '.csv', index = False, header=True)
        #Rename Files that i readed to avoid reading them again  
        os.rename(inputDirectory +'/'+loadFiles , inputDirectory +'/'+ 'Loaded_' + loadFiles) 
    
    else:
    	
    	# Drop Null Values
    	df = df.dropna(axis=0)
    	# To remove .json from file to convert it to csv after that
    	csvFileName = loadFiles.rsplit('.' , 1)
    	#print(csvFileName[0])
    	# Load DataFrame to CSV file
    	n_rows = df.shape[0]
    	print("Number of row of file  "+ loadFiles + " :  " + str(n_rows))
    	df.to_csv (TargetDirectory +'Done_' + csvFileName[0] + '.csv', index = False, header=True)  
    	#Rename Files that i readed to avoid reading them again 
    	os.rename(inputDirectory +'/'+loadFiles , inputDirectory +'/'+ 'Loaded_' + loadFiles) 
    	
    	
total_time = time.time() - startTime
print("Total time: " + str(total_time))
