#!/bin/bash

RUN_LOGS_HOME=~/logs/run_logs
SEED_LOGS_HOME=~/logs/seed_logs
LOGS_FILENAME=`date +"%Y_%m_%d_%H:%M:%S"`

SLACK_NOTIFY_HOME=tools-comms/comms-slack/

STAGE='Initialising'

abort()
{
    echo >&2 '
***************
*** ABORTED ***
***************
'
    echo "An error occurred. Exiting at stage: $STAGE..." >&2
    exit 1
}

trap 'abort' 0

set -e

STAGE="Pulling from main"
# $SLACK_NOTIFY_HOME/notify_slack.sh "$RUNTYPE $STAGE"
echo $STAGE

# git checkout main
# git pull

STAGE="Running data model"
# $SLACK_NOTIFY_HOME/notify_slack.sh "$RUNTYPE $STAGE"
echo $STAGE

# dbt deps
# dbt seed > $SEED_LOGS_HOME/dbt_seed_$LOGS_FILENAME.txt
dbt run > $RUN_LOGS_HOME/dbt_run_$LOGS_FILENAME.txt

STAGE=" data model : finished"
# $SLACK_NOTIFY_HOME/notify_slack.sh "$RUNTYPE $STAGE"
echo $STAGE

trap : 0

echo >&2 '
************
*** DONE ***
************
'
