#!/bin/bash

# no logging for dbt docs generate (not needed)

SLACK_NOTIFY_HOME=tools-comms/comms-slack/

STAGE='Initialising'

abort()
{
    echo >&2 '
***************
*** ABORTED ***
***************
'
   $SLACK_NOTIFY_HOME/notify_slack.sh "$RUNTYPE ERROR & EXIT at stage: $STAGE"
    echo "An error occurred. Exiting at stage: $STAGE..." >&2
    exit 1
}

trap 'abort' 0

set -e

STAGE="Pulling from main"
# $SLACK_NOTIFY_HOME/notify_slack.sh "$RUNTYPE $STAGE"
echo $STAGE

git checkout main
git pull

STAGE="Generating dbt docs"
# $SLACK_NOTIFY_HOME/notify_slack.sh "$RUNTYPE $STAGE"
echo $STAGE

dbt docs generate

STAGE="dbt docs generate : finished"
# $SLACK_NOTIFY_HOME/notify_slack.sh "$RUNTYPE $STAGE"
echo $STAGE

trap : 0

echo >&2 '
************
*** DONE ***
************
'
