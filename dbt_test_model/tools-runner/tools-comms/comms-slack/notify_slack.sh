
MESSAGE=$1

WEBHOOK=https://hooks.slack.com/services/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

PAYLOAD='payload={"text": "'
PAYLOAD+=$MESSAGE
PAYLOAD+='"}'

curl -X POST --data-urlencode "$PAYLOAD" $WEBHOOK
