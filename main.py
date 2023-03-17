import os
import json
import string, random, requests
import time
import pandas as pd
import csv
import io
from flask import Flask, request, redirect, url_for, jsonify, send_file, Response, render_template, flash
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from databricks_api import DatabricksAPI


app = Flask(__name__, instance_relative_config=True)
app.secret_key = "secret key" # for encrypting the session
path = os.getcwd()
# file Upload
UPLOAD_FOLDER = os.path.join(path, 'uploads')
# Make directory if "uploads" folder not exists
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = set(['parquet', 'json'])

connect_str = "DefaultEndpointsProtocol=https;AccountName=f1datalakegen2;AccountKey=/Tq6QcK/D+PapopnlSSYOdRwt2YObLrLQMxR1WP6iN+9zCkweAYl/cX4FF0ko5OMLvFh/qHBpvA0+AStljssVw==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "validated"
# Define your Azure storage account and container information
container_name = 'raw/data'
personal_access_token = 'dapi506245e280d4bb9c0d71c59687a78932'


file_name_dict = {}
container_client = blob_service_client.get_container_client(container='validated')
blob_list = container_client.list_blobs()
for blob in blob_list:
    if blob.name.startswith('output/'):
        li = blob.name.split('/')
        if file_name_dict.get(li[1]) is None:
            file_name_dict[li[1]] = ''
        else:
            file_name_dict[li[1]] = li[2]


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# Define a route to display the file data on a webpage
@app.route('/files', methods=['GET', 'POST'])
def display_files():
    if request.method == 'POST':
        value = request.form.get("query").split('/')
        blob_path = f"output/{value[0]}.csv/{file_name_dict.get(f'{value[0]}.csv')}"
        blob_client = blob_service_client.get_blob_client('validated', blob_path)
        csv_data = blob_client.download_blob().content_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))
        table = df.to_html(classes='table table-striped', index=False)
        return render_template('display.html', table=table, title=value[1])
    return render_template('query.html')


# # Define a route to download a file
# @app.route('/download')
# def download_file():
#     for blob in blob_list:
#         if 'output/' in blob.name:
#             blob_name = blob.name.split('/')[1]
#             blob_client = blob_service_client.get_blob_client(container='validated', blob=blob)
#             with open(file= os.path.join('downloads',blob_name) , mode="wb") as sample_blob:
#                 download_stream = blob_client.download_blob()
#                 sample_blob.write(download_stream.readall())
#     return redirect(url_for('display_files'))


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file_li = []
        files = request.files.getlist("file")

    # Loop through the uploaded files and upload each one to Blob Storage
        for file in files:
            file_li.append(file.filename)
            # Upload the file to Blob Storage
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file.filename)
            blob_client.upload_blob(file.read(), overwrite=True)
        return redirect(url_for('running_status'))
    return render_template('upload.html')


@app.route('/running-process')
def running_status():
    time.sleep(300)
    return redirect(url_for('display_files'))

@app.route('/recommendation', methods=['POST'])
def recommendation():
    userId = int(request.form.get('userId'))
    function_call = '{}()'.format('recommend_movies')
    job_id = 418950021701179
    # Set up the API endpoint and headers
    api_endpoint = f'https://adb-1086592285570595.15.azuredatabricks.net/api/2.0/jobs/run-now'
    headers = {'Authorization': f'Bearer {personal_access_token}', 'Content-Type': 'application/json'}

    # Define the API parameters
    params = {
        "param1": "inputUserId",
        "param2": userId
    }

    data = {
        'job_id': job_id,
        "python_params": json.dumps(params),
    }
    # Make the POST request to start the job
    response = requests.post(api_endpoint, headers=headers, data=json.dumps(data))
    print(response)
    run_id = response.json()["run_id"]
    print(response.json()["run_id"])

    # Set up the API endpoint and headers
    api_endpoint = 'https://adb-1086592285570595.15.azuredatabricks.net/api/2.0/jobs/runs/get'
    headers = {'Authorization': f'Bearer {personal_access_token}', 'Content-Type': 'application/json'}

    # Make the GET request to retrieve the job run details
    status = None
    while status not in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
        response = requests.get(api_endpoint, headers=headers, params={'run_id': run_id})
        status = response.json()['state']['life_cycle_state']
        print(status)
        time.sleep(10)

    # Define the Azure Databricks API endpoint and authentication token
    api_endpoint = f'https://adb-1086592285570595.15.azuredatabricks.net/2.1/jobs/runs/get-output?run_id={run_id}'
    api_token = personal_access_token
    # Create an instance of the Azure Databricks API client
    db = DatabricksAPI(host=api_endpoint, token=api_token)
    output = db.jobs.get_run_output(run_id)
    recommendations = json.loads(output['logs'])
    dict_list = [json.loads(d) for d in recommendations['recommendation']]
    return render_template('recommendation.html', data=dict_list, title='Recommendation', fav=recommendations['favorite_genre'], user=userId)


@app.route('/date-filter', methods=['GET','POST'])
def datefilter():
    startDate = request.form.get('start-date')
    endDate = request.form.get('end-date')
    filter = request.form.get('filter')
    criteria = request.form.get('criteria')
    # typeOfQuery = request.form.get('typeOfQuery')
    print(startDate, endDate, filter, criteria)
    job_id = 156027196770425
    # Set up the API endpoint and headers
    api_endpoint = f'https://adb-1086592285570595.15.azuredatabricks.net/api/2.0/jobs/run-now'
    headers = {'Authorization': f'Bearer {personal_access_token}', 'Content-Type': 'application/json'}

    # Define the API parameters
    params = {
        "startDate": startDate,
        "endDate": endDate,
        "filter": filter,
        "criteria": criteria
    }
    data = {
        'job_id': job_id,
        "python_params": json.dumps(params),
    }
    # Make the POST request to start the job
    response = requests.post(api_endpoint, headers=headers, data=json.dumps(data))
    run_id = response.json()["run_id"]
    # Set up the API endpoint and headers
    api_endpoint = 'https://adb-1086592285570595.15.azuredatabricks.net/api/2.0/jobs/runs/get'
    headers = {'Authorization': f'Bearer {personal_access_token}', 'Content-Type': 'application/json'}

    # Make the GET request to retrieve the job run details
    status = None
    while True:
        response = requests.get(api_endpoint, headers=headers, params={'run_id': run_id})
        status = response.json()['state']['life_cycle_state']
        print(status)
        if status in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
            break
        time.sleep(10)

    # Define the Azure Databricks API endpoint and authentication token
    api_endpoint = f'https://adb-1086592285570595.15.azuredatabricks.net/2.1/jobs/runs/get-output?run_id={run_id}'
    api_token = personal_access_token
    # Create an instance of the Azure Databricks API client
    db = DatabricksAPI(host=api_endpoint, token=api_token)
    output = db.jobs.get_run_output(run_id)
    print(output['logs'])
    output = json.loads(output['logs'])
    dict_list = [json.loads(d) for d in output]
    df = pd.DataFrame(dict_list)
    table = df.to_html(classes='table table-striped', index=False)
    return render_template('display.html', table=table, title=f"{filter} {criteria} Movie between {startDate} and {endDate}")

def id_generator(size=32, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

if __name__ == '__main__':
    app.run()
