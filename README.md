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

More documentation:

Virtual environments:

- Basic steps: https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/
- Create virtual env using `virtualenv venv` (named environment venv)
- Enter virtual env using: `venv/Scripts/Activate.ps1`

Setting up Slack notifications:

- Create a Slack app w default app configs (scopes, bots and stuff)

  - Found it via: https://api.slack.com/tutorials/tracks/getting-a-token?app_id_from_manifest=A045JL75KU5

- Under 'Features and Functionality', allow your app to have incoming web hooks and add a new webhook to your workspace

  - Follow this link: https://www.reply.com/data-reply/en/content/integrating-slack-alerts-in-airflow

- Go to your App's home and make sure it has a bot and a bot token

  - Follow this link: https://api.slack.com/apps/A045JL75KU5/app-home

- Use the bot token to make the API call or to use the Slack operator in your DAG
