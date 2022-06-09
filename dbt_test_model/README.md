Welcome to your dbt project!

# dbt setup

## 1. Local Setup

### Install dbt CLI
Open up the terminal and check if dbt is already installed:
```dbt --version```

If not, follow the instructions [here](https://docs.getdbt.com/dbt-cli/installation/).

### Configure your profile

#### AWS
To be able to connect to a DWH, you have to configure a profile and project file. [This link](https://docs.getdbt.com/docs/configure-your-profile) explains how.

Snowflake example:

```yml
Pensionbee_Data_Model: # profile name
  target: dev # this will be the default target
  outputs:
    dev:
      type: snowflake
      account: <account>
      user: <username>
      password: <password>
      role: PB_DEVELOPER
      warehouse: COMPUTE_WH
      database: PENSIONBEE_ANALYTICS
      schema: <dbt_yourname>
      threads: 12
```

TLDR:
<br>- Go to (or `mkdir`) ~/Users/yourusername/.dbt/. Here you'll find (or create) a file called `profiles.yml`. Fill in the appropriate connection details here, using the example above as template.
<br>- In your project folder you will also find a file called `dbt_project.yml`.
Your profile name needs to be the same as specified in the `profile: 'Pensionbee_Data_Model'` section of the `dbt_project.yml` file.
<br>- cd to the dbt project folder within the repo (i.e. `client-pensionbee/Pensionbee_Data_Model`), run ```dbt debug``` to test to connection and configuration.

## 2. Production (AWS)

### Create an EC2 instance
Create an instance (by default, start with t2.nano and see if that is enough - if not scale up) and ssh to it.

### Install Python and PIP
At the moment (2020-04) Python 2.7 is installed by default, but dbt needs Python 3.
<br>[Here](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html) is some information on how to install Python 3 and PIP (You don't need the EB CLI). Go for the yum option and put `export PATH=~/.local/bin:$PATH` in `.bash_profile` (or modify existing PATH to: `PATH=$HOME/.local/bin:$HOME/bin:$PATH`).

### Install Git
Install Git by running ```sudo yum install git```.

### Install dbt
Install dbt by following their instructions [here](https://docs.getdbt.com/dbt-cli/installation/#pip).

If the EC2 instance is only used to run dbt, setting up a virtual environment might be redundant.

### Clone Git repo
Set your global username and email.
<br> ```git config --global user.name "FIRST_NAME LAST_NAME"```
<br> ```git config --global user.email "YOUR_EMAIL@example.com"```
<br> Generate an ssh key:
<br> ```ssh-keygen -t rsa -b 4096 -C "YOUR_EMAIL@example.com"```
<br> And add it to GitHub
<br> ```cat ~/.ssh/id_rsa.pub```
<br> Test your connection:
<br> ```ssh -T git@github.com```
<br> And clone the repo

### DWH connection
Make sure your `profiles.yml` file is up to date --> see section on Local Setup, but change the `schema` parameter to `schema: dbt_prod`. Also change target and output from dev to prod for the profile on the EC2 instance.

### Set up cronjobs

  - depending on where dbt, pip3 etc got installed (which you can check with e.g. `which dbt`), you may have to modify your `~/.bashrc` file to add the PATH
  e.g `export PATH=/home/dbt-runner/.local/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin` or add the PATH to the crontab file.

  - `cd` back to the dbt user home directory if not already there and create folders for the local logs --> following the current repo configuration and the nested log folder structure, this would be:
      `mkdir logs`
      `mkdir logs/test_logs`
      `mkdir logs/run_logs`
      `mkdir logs/seed_logs`

  - once you have done this, you can set up a  series of cronjobs to have dbt run, test and generate docs on a schedule.\
  These would look something like this (assuming you have cloned the repo inside the home directory):

  ```
  # dbt run (once an hour on the hour)
  00 * * * * cd ~/client-pensionbee/Pensionbee_Data_Model/tools-runner && bash dbt_run.sh > ~/logs/cron_dbt_run.txt 2>&1

  # dbt docs generate (four times a day or manually as needed)
  15 8,12,14,18 * * * cd  ~/client-pensionbee/Pensionbee_Data_Model/tools-runner && bash dbt_docs_generate.sh > ~/logs/cron_dbt_docs_generate.txt 2>&1

  # dbt test (once a day for now)
  30 17 * * * cd ~/client-pensionbee/Pensionbee_Data_Model/tools-runner && bash dbt_test.sh > ~/logs/cron_dbt_test.txt 2>&1
  ```

### Check the logs

  - these are written at: `~/logs` and also dbt runs its own logs at: `~/client-pensionbee/Pensionbee_Data_Model/logs/dbt.log`

### Set up `screen` to serve docs [Optional but recommended]

  - With each run of `dbt docs generate`, dbt creates a json file that can power a static documentation website. To serve this website & have it up at all times (i.e. to avoid it being unavailable wehen you disconnect from the box), you can create a new screen and run `dbt docs serve` in there. For instance:

      - `screen -S dbt_docs_server` [this creates a named screen, easier to recognize]
      - inside the screen, type `dbt docs serve`
      - you can now *detach* the screen by pressing `ctrl+a+d`. This will take you back to your original terminal, but the process in screen will still be running
      - if at any time you want to re-enter, i.e. reattach the screen, type `screen -r`
      - dbt will serve documents on the port 8080 by default. Once this port is configured to accept inbound connections (possibly from a restricted list of ip addresses), the docs will be reachable at *http://external_ip_address_of_the_EC2instance:8080/#!/overview*



### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

--------------------------------------------------------------

# Top level data model documentation

### Attribution model outline:

The scripts in the `domain` folder leverage the rules specified in the CSV seeds in the `data` folder to:
- construct a vertical representation of each atomic event, where each event is expressed through a series of attribute-value pairs as dictated by the `taxonomy` seed.
- classify each of these events as a *conversion* or a *touch* (or none of either), based on the rules specified in `touch_rules` and `conversion_rules` seeds. Moreover, in the context of this model, a touch is only considered as such if it precedes a conversion.
- finally, the attribution model is built assigning weights to each touch based on the rules specified in `attribution_rules` and `conversion_shares` seeds. Currently, these files contain specifications for a W shaped model.

For more information on each step, see the relevant dbt docs.
