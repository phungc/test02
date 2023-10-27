#!/bin/bash

# Define the list of Spark Streaming job names to monitor
job_list=("JobName1" "JobName2" "JobName3")

# Email configuration
recipient_email="youremail@example.com"

# Function to send an email
send_email() {
  subject="$1"
  message="$2"
  echo "$message" | mail -s "$subject" "$recipient_email"
}

# Function to check if it's a weekday (not Saturday or Sunday)
is_weekday() {
  day=$(date +%u)
  [ "$day" -ge 1 ] && [ "$day" -le 5 ]
}

# Initialize an associative array to track job status and monitoring status
declare -A job_status
declare -A job_monitoring

# Function to get the YARN application ID by job name
get_yarn_app_id() {
  job_name="$1"
  yarn_app_id=$(yarn application -list | grep "$job_name" | awk '{print $1}')
  echo "$yarn_app_id"
}

while true; do
  current_day=$(date +%A)

  if is_weekday; then
    for job in "${job_list[@]}"; do
      yarn_app_id=$(get_yarn_app_id "$job")

      if [ -n "$yarn_app_id" ]; then
        status=$(yarn application -status "$yarn_app_id" 2>/dev/null | grep "State" | awk '{print $NF}')
        
        # Check if the job is being monitored
        if [ -z "${job_monitoring[$job]}" ] && [ "$status" != "FAILED" ]; then
          job_monitoring["$job"]=1
          send_email "Job Started Monitoring" "Monitoring of Spark Streaming job $job ($yarn_app_id) has started."
        }

        if [ "$status" == "FAILED" ]; then
          if [ -n "${job_monitoring[$job]}" ]; then
            send_email "Job Failed" "The Spark Streaming job $job ($yarn_app_id) has failed. Monitoring will resume when it starts running again."
            unset job_monitoring["$job"]
          }
        elif [ "$status" == "SUSPENDED" ]; then
          send_email "Job Suspended" "The Spark Streaming job $job ($yarn_app_id) is suspended."
        }
      else
        echo "Job $job not found in YARN applications."
      fi
    done
  else
    echo "It's $current_day. Monitoring is suspended on weekends."
  fi

  sleep 3600  # Sleep for an hour before checking again
done
