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
account_name = 'f1datalakegen2'
account_key = '/Tq6QcK/D+PapopnlSSYOdRwt2YObLrLQMxR1WP6iN+9zCkweAYl/cX4FF0ko5OMLvFh/qHBpvA0+AStljssVw=='
container_name = 'raw/data'
personal_access_token = 'dapi506245e280d4bb9c0d71c59687a78932'
account_url = f"https://{account_name}.blob.core.windows.net"

# Initialize a BlobServiceClient object to interact with the Azure Blob Storage service
blob_uri = f"https://{account_name}.blob.core.windows.net"

container_client = blob_service_client.get_container_client(container='validated')
blob_list = container_client.list_blobs()

file_name_dict = {
"average" : "/output/average.csv/part-00000-tid-1234573884919535624-2bec834d-a17f-4546-b847-113c79ec7063-1861-1-c000.csv",
"totalCountMovies" : "/output/totalCountMovies.csv/part-00000-tid-2286955346304826296-d82dd4aa-6bcc-4459-bc8a-f96fabf2db73-2187-1-c000.csv",
"topGenreMostWatchedMovie" : "/output/topGenreMostWatchedMovie.csv/part-00000-tid-5156291253664288090-a9bb6f30-6dc9-4b97-8780-50f681559105-1417-1-c000.csv",
"topUsers" : "/output/topUsers.csv/part-00000-tid-9062516593703483011-e7a66fc5-c4ab-4536-8e0a-d10b91469ae2-2475-1-c000.csv" ,
"leastView" : "/output/leastView.csv/part-00000-tid-5420422514532730542-ede9c121-be16-40ad-86a8-6ca628f309aa-2761-1-c000.csv",
"topWatchedMovieEachGenre" : "/output/topWatchedMovieEachGenre.csv/part-00000-tid-6370681123938897375-67dd1804-322b-401f-9b03-5b8dc5b4d61d-3049-1-c000.csv",
"leastWatchedMovie" : "/output/leastWatchedMovie.csv/part-00000-tid-5350214237013151245-2228859a-6d7f-4b6f-81f9-8fdb2cb18a1f-3336-1-c000.csv",
"topRatedMovieEachGenre" : "/output/topRatedMovieEachGenre.csv/part-00000-tid-4120065244775218531-33dd8d81-5c35-4451-887d-e3a25b1b5c3e-3531-1-c000.csv",
"leastRatedMovieEachGenre" : "/output/leastRatedMovieEachGenre.csv/part-00000-tid-9005278412169055862-8a6d62a1-bdab-4d91-9bbd-6aa614d68462-3627-1-c000.csv"
}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# Define a route to display the file data on a webpage
@app.route('/files', methods=['GET', 'POST'])
def display_files():
    if request.method == 'POST':
        value = request.form.get("query").split('/')
        # Code to access file from blob storage
        file_path = file_name_dict[value[0]]
        blob_client = blob_service_client.get_blob_client(container='validated', blob=file_path)
        # file_contents = blob_client.download_blob().content_as_text()
        # Download the CSV data as bytes
        csv_data = blob_client.download_blob().content_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))
        table = df.to_html(classes='table table-striped', index=False)

        # read_csv = pd.read_csv(f'downloads/{value[0]}.csv', delimiter=',')  # or delimiter = ';'
        # table = read_csv.to_html(classes='table table-striped', index=False)
        return render_template('display.html', table=table, title=value[1])
    return render_template('query.html')


# # Define a route to download a file
# @app.route('/download')
# def download_file():
#     for blob in blob_list:
#         if 'output/' in blob.name:
#             print(blob)
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


@app.route('/running-status')
def running_status():
    return 'Processing Data'

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
    app.run(debug=True)