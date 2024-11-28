import requests, json, pytz
from datetime import datetime
import subprocess
from subprocess import Popen, PIPE
from configparser import SafeConfigParser
import os
import sys
import time
import configparser
import argparse
from configparser import SafeConfigParser
import logging, traceback
import uuid

def runCommand(cmd):
    print(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    status = p.wait()
    return status

def getFileStat(file_name):
    if os.path.exists(file_name):
        return 1
    else:
        return 0

def deleteFile(filePath):
    '''Delete local files'''
    try:
        os.remove(filePath)
        return 0
    except Exception:
        return 1

def getIdpsKey(secret_name, path):
    '''Fetch secret from IDPS'''
    runUid = str(uuid.uuid1())
    fileName = path
    cmd = "/usr/local/bin/stash get --api-key-grant-policy-id {0} --vkm {1} --secret-name {2} --output {3} --utf-8".format("p-8tdf63ct173x", 'vkm.ps.idps.a.ABC.com', secret_name, fileName)
    status = runCommand(cmd)
    if status == 0:
        if getFileStat(fileName):
            with open(fileName, 'rb') as file:
                file_data = file.read()
                deleteFile(fileName)
                return file_data
        else:
            deleteFile(fileName)
            print("IDPS credentials files [%s] doesn't exiting. Exiting...." % (fileName))
            sys.exit(-1)
    else:
        deleteFile(fileName)
        print("Error encountered while fetching password from IDPS for this script. Exiting ....")
        sys.exit(-1)


def getCollectionAssetId(collection_name, app_secret, ABC_token, ABC_userid):
  '''
  This function recieves collection alias as input and returns the asset id.
  '''
  payload = {}
  get_collection=f"https://idpjobschedulerservice.api.ABC.com/api/v1/collections/{collection_name}"
  response=requests.request("GET", get_collection, headers=headers, data=payload)
  headers = {
      'Authorization': f'ABC_IAM_Authentication ABC_token_type=IAM-Ticket,ABC_appid=ABC.data.finance.finmdrcrawler,ABC_app_secret={app_secret},ABC_token={ABC_token},ABC_userid={ABC_userid}',
      'Cookie': 'apip=123146153589859'
    }
  
  return json.loads(response.text)['assetConfiguration']['assetId']

def run_cmd(cmd):
    encoding = 'utf-8'
    status=-1
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    status = process.returncode
    return status, stdout.decode(encoding), stderr.decode(encoding)


extract_time=datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y_%m_%d_%H_%M_%S") #IST time for file name
try:
    parser = argparse.ArgumentParser()
    #print(status)
    parser.add_argument("-t", "--token", required=True, type=str)
    parser.add_argument("-u", "--userid", required=True, type=str)
    args = parser.parse_args()
    print(args)
    if args.token is None or args.userid is None:
        print("Either token or s3 user id not provided. Exiting...")
        sys.exit(-1)
    ABC_token = args.token
    ABC_userid = args.userid
    
    app_secret = getIdpsKey('fdl/bpp/app_secret', '/tmp/temp_key').decode("utf-8")
    
    payload = {}
    headers = {
      'Authorization': f'ABC_IAM_Authentication ABC_token_type=IAM-Ticket,ABC_appid=ABC.data.finance.finmdrcrawler,ABC_app_secret={app_secret},ABC_token={ABC_token},ABC_userid={ABC_userid}',
      'Cookie': 'apip=123146153589859'
    }
    
    list_porject_url = "https://idpjobschedulerservice.ABC.ABC.com/api/v1/assets/projects"
    project_response = requests.request("GET", list_porject_url, headers=headers, data=payload)
    list_of_assets=[]
    if project_response.ok:
      print("Connection Successful")
      for each_project in json.loads(project_response.text):
        if each_project['gitOrg'] == 'data-finance':
          project_name=each_project['name']
          project_asset_id=each_project['assetId']
          list_assets_url = f"https://idpjobschedulerservice.api.ABC.com/api/v2/pipelines?environment=PRD&projectAssetId={project_asset_id}&size=1500"
          list_asset_response = requests.request("GET", list_assets_url, headers=headers, data=payload)
          if list_asset_response.ok:
            if json.loads(list_asset_response.text)['numberOfElements']>0:
              for each_asset in json.loads(list_asset_response.text)['content']:
                pipeline_name_url = f"https://idpjobschedulerservice.api.ABC.com/api/v1/pipelines/{each_asset['pipelineName']}"
                pipeline_response = requests.request("GET",pipeline_name_url,headers=headers, data=payload)
                output_file = f"{each_asset['pipelineName']}.json"
                output = json.dumps(json.loads(pipeline_response.text), indent=4)
                with open(output_file, 'w+', encoding='utf-8') as file:
                  json.dump(json.loads(pipeline_response.text), file, indent=4)
                cp_cmd='aws s3 mv '+output_file+' '+f's3://idl-finance-sandbox-uw2-processing-fin-prd/bpp-jsons/{project_name}/{output_file}'
                print(cp_cmd)
                status = run_cmd(cp_cmd)
                print(f"File Generated : {output_file}")
    else:
      print(str(project_response.status_code)+": Unable to connnect")
except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    print(''.join('!! ' + line for line in lines))
    print(str(e))
    print("Required Input Argument Not Passed Properly. Exiting...")
    sys.exit(-1)
