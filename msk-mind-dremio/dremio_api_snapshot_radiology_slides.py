#!/usr/bin/env python3

'''
   dremio_api_snapshot_radiology_slides.py : an example of using the Dremio WEB API
   Copyright (C) 2022 Memorial Sloan Kettering Cancer Center.
  
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.
  
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.
 
   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.
'''

'''
   This is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
   FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
   is on an "as is" basis, and Memorial Sloan Kettering Cancer Center has no
   obligations to provide maintenance, support, updates, enhancements or
   modifications. In no event shall Memorial Sloan Kettering Cancer Center be
   liable to any party for direct, indirect, special, incidental or
   consequential damages, including lost profits, arising out of the use of this
   software and its documentation, even if Memorial Sloan Kettering Cancer
   Center has been advised of the possibility of such damage.
'''

import json
import requests
import sys
import time

PROPERTY_FILENAME = "dremio_api_properties.json"

def print_properties_file_missing_message():
    sys.stderr.write("Error : could not find required property file " + PROPERTY_FILENAME + "\n")
    sys.stderr.write("  file should be formatted in json using this template:\n")
    properties_file_template = { "access_token": "token_if_you_have_one_or_can_be_empty", "dremio_base_url": "https://your_dremio_server_domain_name", "password": "your_dremio_password", "username": "your_dremio_username", "verify_certificates": "True_or_False" }
    sys.stderr.write(json.dumps(properties_file_template, indent=4, sort_keys=True))
    sys.stderr.write("\n")

def validate_required_property(error_msg_buffer, props, property_key):
    if not property_key in props:
        error_msg_buffer.append("property '" + property_key + "' is required but missing from the properties file.")

def validate_properties(props):
    error_msg_buffer = []
    validate_required_property(error_msg_buffer, props, "dremio_base_url")
    validate_required_property(error_msg_buffer, props, "username")
    validate_required_property(error_msg_buffer, props, "password")
    validate_required_property(error_msg_buffer, props, "verify_certificates")
    if (len(error_msg_buffer) > 0):
        sys.stderr.write("\n".join(error_msg_buffer) + "\n")
        return False
    else:
        return True

def read_properties():
    try:
        with open(PROPERTY_FILENAME, "r") as prop_file:
            props = json.load(prop_file)
    except FileNotFoundError:
        print_properties_file_missing_message()
        sys.exit(1)
    if validate_properties(props):
        return props
    else:
        sys.stderr.write("Properties were read but were invalid. Exiting.\n")

def overwrite_properties(props):
    with open(PROPERTY_FILENAME, "w") as prop_file:
        if validate_properties(props):
            prop_file.write(json.dumps(props, indent=4, sort_keys=True))
            prop_file.write("\n")
        else:
            sys.stderr.write("Properties could not be written because they are invalid. Exiting.\n")

def verifying_certificates(props):
    verify = True
    if props["verify_certificates"] == "False":
        verify = False
    return verify

def disable_warnings_if_not_verifying_certificates(props):
    if not verifying_certificates(props):
        requests.packages.urllib3.disable_warnings()

def raw_request(props, url, post_data, response_data, inhibit_access_token_header=False):
    verify = verifying_certificates(props)
    headers = { }
    headers["Content-Type"] = "application/json"
    if not inhibit_access_token_header:
        headers["Authorization"] = "_dremio" + props["access_token"]
    response = None
    if post_data == None:
        response = requests.request("GET", url, headers=headers, verify=verify)
    else:
        response = requests.request("POST", url, headers=headers, json=post_data, verify=verify)
    if response != None:
        if response.status_code == 200:
            response_json = response.json()
            response_data.clear()
            response_data.append(response_json)
    return response.status_code

def request_source_url(props):
    return props["dremio_base_url"] + "/api/v3/source"

def request_login_url(props):
    return props["dremio_base_url"] + "/apiv2/login"

def request_list_catalogs_url(props):
    return props["dremio_base_url"] + "/api/v3/catalog"

def request_catalog_url(props, catalog_id):
    return props["dremio_base_url"] + "/api/v3/catalog/" + catalog_id

def request_run_sql_url(props):
    return props["dremio_base_url"] + "/api/v3/sql"

def request_job_url(props, job_id):
    return props["dremio_base_url"] + "/api/v3/job/" + job_id

def request_job_results_page_url(props, job_id, start_offset, limit):
    return props["dremio_base_url"] + "/api/v3/job/" + job_id + "/results?offset=" + str(start_offset) + "&limit=" + str(limit)

def request_source_list(props, response_data):
    return raw_request(props, request_source_url(props), None, response_data)

def request_list_catalogs(props, response_data):
    return raw_request(props, request_list_catalogs_url(props), None, response_data)

def request_catalog(props, catalog_id, response_data):
    return raw_request(props, request_catalog_url(props, catalog_id), None, response_data)

def request_run_sql(props, sql, response_data):
    post_data = dict({"sql": sql})
    return raw_request(props, request_run_sql_url(props), post_data, response_data)

def request_job(props, job_id, response_data):
    return raw_request(props, request_job_url(props, job_id), None, response_data)

def request_job_results_page(props, job_id, start_offset, limit, response_data):
    return raw_request(props, request_job_results_page_url(props, job_id, start_offset, limit), None, response_data)

def token_is_defined(props):
    return "access_token" in props and len(props["access_token"]) > 1

def token_is_usable(props):
    status_code = request_source_list(props, [])
    if status_code == 401:
        sys.stderr.write("authorization failed for url " + request_source_url(props) + "\n")
    return status_code == 200

def request_token(props):
    response_data = []
    url = request_login_url(props)
    post_data = dict({"userName": props["username"], "password": props["password"]})
    status_code = raw_request(props, url, post_data, response_data, inhibit_access_token_header=True)
    if status_code == 401:
        sys.stderr.write("attempt to generate access token failed .. check username/password in properties\n")
        sys.exit(1)
    if status_code != 200:
        sys.stderr.write("attempt to generate access token failed. status code " + str(status_code) + "\n")
        sys.exit(1)
    if len(response_data) != 1 or 'token' not in response_data[0] or len(response_data[0]['token']) == 0:
        sys.stderr.write("token generation response did not contain any token\n")
        sys.exit(1)
    props['access_token'] = response_data[0]['token']
    return True

def get_new_access_token(props):
    if request_token(props):
        if token_is_usable(props):
            overwrite_properties(props)
        else:
            sys.stderr.write("token generation endpoint returned an invalid token\n")
            sys.exit(1)

def update_access_token(props):
    sys.stderr.write("refreshing access token\n")
    get_new_access_token(props)
    return True

def update_access_token_if_necessary(props):
    if not token_is_defined(props) or not token_is_usable(props):
        update_access_token(props)

def list_catalogs(props):
    response_data = []
    status_code = request_list_catalogs(props, response_data)
    if status_code == 200:
        return response_data[0]["data"]
    else:
        sys.stderr.write("attempt to list catalogs failed. status code " + str(status_code) + "\n")
        sys.exit(1)

def get_catalog(props, catalog_id):
    response_data = []
    status_code = request_catalog(props, catalog_id, response_data)
    if status_code == 200:
        return response_data[0]
    else:
        sys.stderr.write("attempt to get catalog " + catalog_id + " failed. status code " + str(status_code) + "\n")
        sys.exit(1)

def run_sql(props, sql):
    response_data = []
    status_code = request_run_sql(props, sql, response_data)
    if status_code == 200 and len(response_data) > 0:
        return response_data[0]
    else:
        sys.stderr.write("attempt to run sql " + sql + " failed. status code " + str(status_code) + "\n")
        sys.exit(1)

def get_job(props, job_id):
    response_data = []
    status_code = request_job(props, job_id, response_data)
    if status_code == 200 and len(response_data) > 0:
        return response_data[0]
    else:
        sys.stderr.write("attempt to get status of job " + job_id + " failed. status code " + str(status_code) + "\n")
        sys.exit(1)

def get_job_state(job, job_id):
    if "jobState" in job:
        return job["jobState"]
    else:
        sys.stderr.write("response for job " + job_id + " did not include jobState as required.\n")
        sys.exit(1)

def get_job_error_message(job, job_id):
    if "errorMessage" in job:
        return job["errorMessage"]
    else:
        sys.stderr.write("response for job " + job_id + " did not include errorMessage field as required.\n")
        sys.exit(1)

def get_job_results_page(props, job_id, job_results, start_offset, limit):
    response_data = []
    status_code = request_job_results_page(props, job_id, start_offset, limit, response_data)
    # NOTE : we could have also parsed fields/schema for job from this response
    if status_code == 200 and len(response_data) > 0:
        for x in response_data[0]["rows"]:
            job_results.append(x)
    else:
        sys.stderr.write("attempt to get status of job " + job_id + " failed. status code " + str(status_code) + "\n")
        sys.exit(1)

def get_job_results(props, job_id, job_results):
    job = get_job(props, job_id)
    if not "rowCount" in job:
        sys.stderr.write("response for completed job " + job_id + " did not include rowCount field as required.\n")
        sys.exit(1)
    rowCount = job["rowCount"]
    start_offset = 0
    while start_offset < rowCount:
        limit = 256
        if start_offset + limit  > rowCount:
            limit = rowCount - start_offset
        get_job_results_page(props, job_id, job_results, start_offset, limit)
        start_offset += limit
    if len(job_results) != rowCount:
        sys.stderr.write("completed job " + job_id + " has " + str(rowCount) + " rows, but obtained results have only " + str(len(job_results)) + " records.\n")
        sys.exit(1)

def job_ended(current_job_state):
    return current_job_state in {"COMPLETED", "CANCELED", "FAILED"}

def get_fields_from_dataset_catalog(scan_annotation_table_deid_dataset_catalog):
    if "fields" in scan_annotation_table_deid_dataset_catalog:
        return [x["name"] for x in scan_annotation_table_deid_dataset_catalog["fields"]]

def print_data_tab_delimited(fields, data):
    sys.stdout.write("\t".join(fields) + "\n")
    for record in data:
        record_fields = [str(record[field]) for field in fields]
        sys.stdout.write("\t".join(record_fields) + "\n")

def get_catalog_for_scan_annotation_table_deid_dataset(props):
    # TODO : this method would be better implemented as taking a Dremio path list as an argument rather than hardcoding
    # find space 'BR_16_512' in top level catalogs and obtain the id
    catalogs = list_catalogs(props)
    BR_16_512_space_id = None
    for x in catalogs:
        if x["path"] == ["BR_16-512"]:
            BR_16_512_space_id = x["id"]
    if BR_16_512_space_id == None:
        sys.stderr.write("could not find space BR_16-512\n")
        sys.exit(1)
    # find folder 'staging' in the 'BR_15_512' space
    BR_15_512_catalog = get_catalog(props, BR_16_512_space_id)
    BR_15_512_staging_folder_id = None
    for c in BR_15_512_catalog["children"]:
        if c["path"] == ['BR_16-512', 'staging']:
            BR_15_512_staging_folder_id = c["id"]
    if BR_15_512_staging_folder_id == None:
        sys.stderr.write("could not find folder BR_16-512.staging\n")
        sys.exit(1)
    # find folder 'radiology' in the 'staging' folder
    BR_15_512_staging_catalog = get_catalog(props, BR_15_512_staging_folder_id)
    BR_15_512_staging_radiology_folder_id = None
    for c in BR_15_512_staging_catalog["children"]:
        if c["path"] == ['BR_16-512', 'staging', 'radiology']:
            BR_15_512_staging_radiology_folder_id = c["id"]
    if BR_15_512_staging_radiology_folder_id == None:
        sys.stderr.write("could not find folder BR_16-512.staging.radiology\n")
        sys.exit(1)
    # find dataset 'scan_annotation_table_deid_dataset_id' in the 'radiology' folder
    BR_15_512_staging_radiology_catalog = get_catalog(props, BR_15_512_staging_radiology_folder_id)
    scan_annotation_table_deid_dataset_id = None
    for c in BR_15_512_staging_radiology_catalog["children"]:
        if c["path"] == ['BR_16-512', 'staging', 'radiology', 'scan_annotation_table_deid']:
            scan_annotation_table_deid_dataset_id = c["id"]
    if scan_annotation_table_deid_dataset_id == None:
        sys.stderr.write("could not find folder BR_16-512.staging.radiology.scan_annotation_table_deid\n")
        sys.exit(1)
    # verify that we found the VDS by checking its entity type
    scan_annotation_table_deid_dataset_catalog = get_catalog(props, scan_annotation_table_deid_dataset_id)
    if not "entityType" in scan_annotation_table_deid_dataset_catalog:
        sys.stderr.write("could not find catalog entry for BR_16-512.staging.radiology.scan_annotation_table_deid\n")
        sys.exit(1)
    scan_annotation_table_deid_entity_type = scan_annotation_table_deid_dataset_catalog["entityType"]
    if scan_annotation_table_deid_entity_type != "dataset":
        sys.stderr.write("BR_16-512.staging.radiology.scan_annotation_table_deid is not of type dataset : entityType is " + scan_annotation_table_deid_entity_type + "\n")
        sys.exit(1)
    return scan_annotation_table_deid_dataset_catalog
    # NOTE : we could also have used the endpoint GET /api/v3/catalog/by-path/{path} with the full path, rather than descending the containers one step at a time

def run_sql_query(props, sql):
    response = run_sql(props, sql)
    job_id = None
    if "id" in response:
        job_id = response["id"]
    if job_id == None or len(job_id) == 0:
        sys.stderr.write("could not obtain job id after running sql " + dataset_sql + "\n")
        sys.exit(1)
    return job_id

def wait_for_job_completion(props, job_id):
    iteration = 0
    max_iterations = 120 # 10 minutes (120 * 5 seconds)
    current_job_state = "UNKNOWN"
    job_error_message = "UNKONWN"
    while not job_ended(current_job_state):
        if iteration != 0:
            time.sleep(5)
        iteration += 1
        job = get_job(props, job_id)
        current_job_state = get_job_state(job, job_id)
        job_error_message = get_job_error_message(job, job_id)
        if iteration == max_iterations:
            break
    if current_job_state != "COMPLETED":
        if job_ended(current_job_state):
            sys.stderr.write("job failed or was canceled for sql " + sql + "\n")
            sys.stderr.write("error message was : " + job_error_message)
            sys.exit(1)
        else:
            sys.stderr.write("maximum wait time exceeded and job had not completed for sql " + sql + "\n")
            sys.exit(1)

def run_sql_query_and_wait_for_completion(props, sql):
    job_id = run_sql_query(props, sql)
    wait_for_job_completion(props, job_id)
    return job_id

def main():
    props = read_properties()
    disable_warnings_if_not_verifying_certificates(props)
    update_access_token_if_necessary(props)
    scan_annotation_table_deid_dataset_catalog = get_catalog_for_scan_annotation_table_deid_dataset(props)
    # NOTE: scan_annotation_table_deid_dataset_catalog["sql"] cannot be used because the underlying files are not visible to all users
    # the dataset path itself can be queried so long as it is visible to the user however
    scan_annotation_table_deid_dataset_sql = "SELECT * FROM \"BR_16-512\".\"staging\".\"radiology\".\"scan_annotation_table_deid\""
    scan_annotation_table_deid_job_id = run_sql_query_and_wait_for_completion(props, scan_annotation_table_deid_dataset_sql)
    # get results of job
    scan_annotation_table_deid_data = []
    get_job_results(props, scan_annotation_table_deid_job_id, scan_annotation_table_deid_data)
    # capture header/fields from catalog
    scan_annotation_table_deid_fields = get_fields_from_dataset_catalog(scan_annotation_table_deid_dataset_catalog)
    # output the snapshot
    print_data_tab_delimited(scan_annotation_table_deid_fields, scan_annotation_table_deid_data)

if __name__ == "__main__":
    main()
