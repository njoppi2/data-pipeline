# 🛢️➡️ Data Engineering Pipeline
## Overview
This data engineering project automates the extraction of data from two sources, a Postgres database, and a CSV file, and loads it into another Postgres database. The goal is to create a pipeline that can be run daily, while maintaining idempotency of tasks and clear isolation between steps.

## Usage Instructions
To set up and run the data engineering pipeline, follow these steps:

1. Clone this repository to your local machine.
2. Make sure you have Docker Compose installed.
3. Open a terminal and navigate to the project directory.
4. Run the following command to setup and start everything:

    ```
    make start
    ```
    And afterwards, you can open another terminal and stop everything with:
    ```
    make stop
    ```

It can take 1-2 minutes for Airflow to fully start. You can monitor the progress in the same terminal you ran `make start`.

Once the containers are up and running, open a web browser and go to [http://localhost:8080/](http://localhost:8080/) to access the Apache Airflow web interface. If the web interface doesn't load, it means Airflow is still starting, if it loads, it means Airflow has fully started and we're ready to authenticate.

To authenticate, use:
- User: airflow
- Password: airflow  

In the Airflow web interface, you will find two DAGs representing the data pipeline:
<br/><img src="https://github.com/njoppi2/data-pipeline/assets/16853682/56442fe7-7f2e-41fa-af1f-8d1bcfba1233" alt="drawing" width="700"/><br/>

- **data_extraction_and_local_storage**: This DAG handles Step 1 of the challenge, which involves extracting data from the Postgres database and CSV file.
- **data_loading_to_final_database**: This DAG manages Step 2, which loads the extracted data to Postgres.
Click on the "play" button for the DAG you want to execute.

A calendar view will appear. Select a date as a parameter for the execution of the DAG and click "Trigger" to start the pipeline.
<br/><img src="https://github.com/njoppi2/data-pipeline/assets/16853682/d608b96b-5b69-43a2-97a3-5cafb799d460" alt="drawing" width="700"/><br/>
After you click you'll be redirected back to the home page, where you'll be able to see the status of the running DAG.

### Error debugging
In case one of your DAG's tasks fail, you can check the logs to help you better understand what happened.
<br/><img src="https://github.com/njoppi2/data-pipeline/assets/16853682/1c50b7c0-10d8-4630-9c81-3e2961cd81b4" alt="drawing" width="700"/><br/>

### Running locally
In the [main.ipynb](https://github.com/njoppi2/data-pipeline/blob/main/main.ipynb) file, you can run the tasks locally, and check the query result.

<!--
The project ensures that each task is idempotent, allowing you to rerun the pipeline without causing duplicates or errors.
Dependencies between steps are enforced to prevent running Step 2 without the successful completion of Step 1.
The pipeline can be configured to run for different days, including past days.
Results of the final query can be found in the query/result.csv file.
