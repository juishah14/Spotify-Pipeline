Data Pipeline

Requires docker and docker-compose (docker desktop offers both).

Start the engine on docker desktop to ensure docker is running (you may need to add yourself to a docker group if you do not have admin permissions on your laptop).

The following steps were taken from: https://www.youtube.com/watch?v=aTaytcxy2Ck&ab_channel=DatawithMarc

Steps:
Run the following commands on your terminal.

1. `docker-compose build`
2. `docker-compose up`

Note:
If it is your first time running this locally and the above steps do not work, you may need to install the dependencies listed in the `requirements.txt` file and perform the steps below first, before trying to build the image again.

1. `docker-compose up airflow-init`
2. `docker-compose down`
3. `docker-compose up`

View state of container: `docker ps` (wait till you see healthy)
View the UI in a browser, go to `localhost:8080`

To add or remove dags while docker-compose is running, simply add or remove them from your local dags folder and wait for a few minutes till the UI refreshes. The dags require some variables, which you may need to add to Airflow via the UI under Admin -> Variables. These variables can be found in the env file.

When you're done, `docker-compose down` and close docker desktop.

Enter virtual env using: `venv/Scripts/Activate.ps1`
