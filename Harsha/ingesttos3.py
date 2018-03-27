import requests
import json
#import boto3

api_token    = "b09d70ce-d201-4fb9-b3c9-5cd19154f3b7"
api_base_url = "https://api.appetizeapp.com/v5/report/venues/"

#Create a dictionary to store all the Venue ids
venuedict = {'Dodgers':'308','Univ':'637','VVCC':'387','BMO':'239','AR':'500'}

#file path to output the details
filepath = "D:/Projects/Support/Harsha/data.json"

try:
    #Open a file in write mode
    with open(filepath, 'w') as outfile:
        #Iterate through each item in dictionary
        for key in venuedict:
            #call rest api
            response = requests.get(api_base_url + venuedict[key] + "?api_key=" + api_token)
            print(response.json())
            strdata =  json.dumps(response.json())
            #write the json response to output file
            outfile.write(strdata)
    #close the file
    outfile.close()  
    print("\nJson file created...")  
    
     
    #putting file to S3
    
    #s3 = boto3.resource('S3')
    ## Upload a json file to s3 bucket
    #file_to_upload = open(filepath, 'rb')
    #s3.Bucket('your-bucket-name').put_object(Key='test.json', Body=file_to_upload)
    
    #print("File uploaded Successfully.")
    
except:
    print("An error occurred")

