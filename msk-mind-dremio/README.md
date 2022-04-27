## demo of using various Dremio API endpoints
This shows an instructive example of using the Dremio API endpoints via python to take periodic snapshots of a virtual dataset. For individual descriptions of the Dremio API endpoints, with example requests and responses, see the Dremio API documentation [here](https://docs.dremio.com/software/rest-api/overview/).

This example runs successfully on the MSK MIND Dremio dev server at https://tlvimskmindvm1 (running Dremio Enterprise Edition 19.3.0-202201011605440679-67815cf6)

### use case
The imagined use case which this example addresses is an automate periodic [weekly] examination of a virtual dataset, perhaps to report on changes since the last examination. The fundamental need is to capture the contents of a VDS in a snapshot of some kind for further analysis.

### incidental needs

#### authentication and access token management
In order to use the API, a valid access token must be obtained from the Dremio server. Access tokens are valid for a limited time and must be renewed when they expire.

#### working around self-signed certificates
Our MSK MIND Dremio server currently has a self-signed certificate which fails to be verified by the standard certificate authority mechanism built in to python.

### list of api endpoints used in demo
* **/apiv2/login** : used for obtaining an access token
* **/api/v3/source** : used to retrieve a list of available sources
* **/api/v3/catalog** : used to retrieve all top level container directories
* **/api/v3/catalog/{id}** : used to retrieve the children within various containers, and the metadata details of a dataset (like schema and original sql)
* **/api/v3/sql** : used to run a mysql query, such as getting up to date output from a VDS - returns a job_id of the running job
* **/api/v3/job/{id}** : used to monitor the progress and retrieve metadata of a job
* **/api/v3/job/{id}/results?offset={start_offset}&limit={record_count_limit}** : used to retrieve the data from a completed job

### high level overview of the example python script
The script (named dremio_api_snapshot_radiology_slides.py) is hardcoded to retrieve a specific VDS from the Dremio server. In Dremio, entities are located inside containers such as spaces and folders, which can be nested. Finding the VDS to process requires beginning at a top level space called BR_15-512, and going into a folder inside that space called "staging", which contains a folder called "radiology" which contains the VDS. The Dremio path through these containers is represented like this: "BR_16-512"."staging"."radiology"."scan_annotation_table_deid" - the final element being the VDS name.

The script uses a secondary file (called dremio_api_properties.json) to persist needed credentials and settings. If that file does not exist, the user is prompted to create it, and a template (in json) is provided:

    {
        "access_token": "token_if_you_have_one_or_can_be_empty",
        "dremio_base_url": "https://your_dremio_server_domain_name",
        "password": "your_dremio_password",
        "username": "your_dremio_username",
        "verify_certificates": "True_or_False"
    }

The script will use the username and password to refresh the access_token if the stored access_token is no longer valid, and will update the properties file with the new (valid) access token when necessary. This allows the script to reuse valid tokens until they expire. One enhancement would be to detect "soon-to-expire" tokens and to refresh them before they expire. In any case, once the user has put their credentials into this properties file, the script will manage the access token renewal process automatically.

The script outputs the contents of the VDS to the (unix) standard output stream and prints diagnostic errors to the standard error stream. The standard output stream can be redirected into a file by running the script with a command such as:

    ./dremio_api_snapshot_radiology_slides.py > ./scan_annotation_table_deid_2022_04_24_snapshot.txt

output is in tab delimited format with a prefixed header line.

This example was limited to data access endpoints, but the Dremio API has many other endpoints such as those for creating entities (such as datasets, spaces, folders) and managing resources, reflections, and user authorities.

### script implementation
Only a single script-scope data variable is defined: a constant to hold the filename of the required properties file. All other variables are local variables, or arguments passed to functions. Once loaded the properties from the property file are passed around through most functions in an argument named "props".

All logic in the script exists inside of defined functions. On entry, the function main() is executed, which performs these high level steps:
1. read the properties from the property file
1. update the access token if it is missing or no longer valid
1. find the VDS catalog entry, which validates that it is present and accessible to the user
1. launch a job to run a query to generate the results from the VDS, and then wait for that job to complete
1. retrieve the records generated by the job and check that the correct record count was retrieved
1. extract the column/field names from the VDS catalog entry
1. print the field names (tab delimited) and the generated records (tab delimited) to the standard output stream

As an implementation pattern, there are functions which generate the endpoint urls and attach url path parameters (one function per endpoint), and there are functions for constructing the http requests (one function per endpoint). All of the request construction functions leverage a single function called "raw_request" which encapsulates the attachment of the necessary header which supplies the access token. raw_request takes an argument "response_data", which should be a list reference from the caller which will be cleared and then populated with the API response (as an object in json format). An http request which fails to generate a "200" / OK status causes the script to not populate response_data and to return the received status code.

Retrieving the results from a completed job is done with the endpoint which retrieves limited subsets of the complete result set, making multiple requests to "page" through the full result set, aggregating a full set of retrieved results on the system executing the script.

There are notes within the script suggesting alternatives and improvements. The script could be readily generalized to take a snapshot of any VDS for example.

#### notes
* On reading the property file, the required fields are verified as being present (see function validate_properties)
* Generating a fresh token is done via an appropriate post request to the login endpoint. An authentication failure is specifically reported as a problem with the supplied username/password.
* validating the access token is done by attempting a request to the source list endpoint. Aside from the http response status code, the results of the request are ignored.
* when waiting for a job to complete, a maximum wait time is allowed (10 minutes). If the job is not completed by the end of the maximum wait, the script exits with a relevant error message.
