# Introduction
Intro of the project.


# Getting Started


## 1. Installation process

Explain how to install the app.


## 2. Environment config

Explain how to configure the app environment (env vars).


### APP_ENV options:
* Development - when developing in local machine
* Staging - running in the Staging machine
* Production - running in the Production machine

## 3. App config

Explain how to configure the app (config.json file).


## 4. Scheduler config
The scheduler is running on a different thread from the main thread.


It's using apscheduler package to manage the schedule with cron simmilar API.
The scheduler is being configured with jobs that send tasks to the ETL API. Those tasks trigger the ETL Runs.


## 5. REQUESTS TYPE

The http request structure:

`http://[address]:[port_number]/[AppName]/ETLRun?task=[request_type]`

There are 2 get requests types, the scheduler sending to the ETL app:

#### [Ping]:
Check if service is alive.

Will always return - [Alive]


#### [App Run]:
A request to start an ETL run. The responses to this task are:

[success] – the ETL run was successful.

[fail] – a severe error was raised in the ETL run, therefore the run was not completed.
