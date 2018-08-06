# BigEye

## Quickstart

### Installation
Install bigeye in your project venv using pip:
```
pip install git+https://github.com/BBOXX/BigEye.git
```

### Set up
You will need to provide a config file in the yaml format with following format in your project directory.
```
Fetchers:
    [nameOfFetcher1]:
      type: PostgresDB
      host: []
      database: []
      port: []
      user: []
      password: []
    [nameOfFetcher2]:
      type: PostgresDB
      host: []
      database: []
      port: []
      user: []
      password: []

Publishers:
  [nameOfDDPublisher]:
    type: Datadog
    apiKey: []
    appKey: []
    batchSize: 2000


runConfiguration:
  fetchers: 
    - [nameOfFetcher1]
    - [nameOfFetcher2]
  publishers:
    - [nameOfDDPublisher]
  types: 
    - consistency
    - quality
  batchSize: 20
  maxTestDuration: 30
  timeBetweenCalls: 10
  iterations: 10
```

You will also need to provide tests descriptions in yaml files located in a folder in your project.
The yaml files for quality checks (one fetcher per test) need to be formatted as follows:
```
name: [name_of_the_metric (USE ONLY UNDERSCORES, NO SPACES OR DASHES)]
description: [description of the metric]
type: [quality]
team: [your_team]
metrics:
  metric1:
    active: [choose between true or false, if false this metric will be ignored]
    fetchers:
      [fetcher_name]:
        [nameOfFetcherDetail1]: [ValueOfFetcherDetail1]
        [nameOfFetcherDetail2]: [ValueOfFetcherDetail2]
    publishers:
      [publisher1Name]:
        [nameOfPublisherDetail1]: [valueOfPublisherDetail1]
        [nameOfPublisherDetail2]: [valueOfPublisherDetail2]
    tags:
      [tag1Name]: [tag1Value]
      [tag2Name]: [tag2Value]
  metric2:
    active: [choose between true or false, if false this metric will be ignored]
    fetchers:
      [fetcher_name]:
        [nameOfFetcherDetail1]: [ValueOfFetcherDetail1]
        [nameOfFetcherDetail2]: [ValueOfFetcherDetail2]
    publishers:
      [publisher1Name]:
        [nameOfPublisherDetail1]: [valueOfPublisherDetail1]
        [nameOfPublisherDetail2]: [valueOfPublisherDetail2]
    tags:
      [tag1Name]: [tag1Value]
      [tag2Name]: [tag2Value]
```

The yaml files for consistency checks (two fetchers per test) need to be formatted as follows:
```
name: [name_of_the_metric (USE ONLY UNDERSCORES, NO SPACES OR DASHES)]
description: [description of the metric]
type: [consistency]
action: [typeOfAction (difference or division)]
team: [your_team]
metrics:
  metric1:
    active: choose between true or false, if false this metric will be ignored
    fetchers:
      [fetcher_name]:
        [nameOfFetcherDetail1]: [ValueOfFetcherDetail1]
        [nameOfFetcherDetail2]: [ValueOfFetcherDetail2]
      [fetcher2Name]:
        [nameOfFetcherDetail1]: [ValueOfFetcherDetail1]
    publishers:
      [publisher1Name]:
        [nameOfPublisherDetail1]: [valueOfPublisherDetail1]
        [nameOfPublisherDetail2]: [valueOfPublisherDetail2]
    tags:
      [tag1Name]: [tag1Value]
      [tag2Name]: [tag2Value]
  metric2:
    fetchers:
      [fetcher_name]:
        [nameOfFetcherDetail1]: [ValueOfFetcherDetail1]
        [nameOfFetcherDetail2]: [ValueOfFetcherDetail2]
      [fetcher2Name]:
        [nameOfFetcherDetail1]: [ValueOfFetcherDetail1]
    publishers:
      [publisher1Name]:
        [nameOfPublisherDetail1]: [valueOfPublisherDetail1]
        [nameOfPublisherDetail2]: [valueOfPublisherDetail2]
    tags:
      [tag1Name]: [tag1Value]
      [tag2Name]: [tag2Value]
```


Depending on what type of fetchers and publishers, they require different fields as described below:

#### Fetchers
- PostgreSQL
The fetcher format for pg in the yaml file is as follows, query text can be multiline as in the example below to make it more readable, just make sure indentation is correct.
```
pg_fetcher_name:
        query: select count(*) from whatever where
          whatever.status is 'not good'
```
- API
tbd
#### Publishers
-Datadog
The publisher details need to contain a dashboardName and a typeOfDahsboard.
```
publisher_name:
        dashboardName: Name of the dashboard in which this metric will be included, it will be created if it does not exist
        typeOfDashboard: Choose between timeboard and screenboard
```

### Usage

```
from bigeye import BigEye


# Runs the tests and output the result
runner = BigEye('dev', 'master', 'config.yaml', './tests/**/*.yaml')
runner.executeResponsabilites()
runner.tearDown()

# Updates the dashboards, necessary if tests have been added or removed
updater = BigEye('dev', 'updateBoards', 'config.yaml', './tests/**/*.yaml')
updater.executeResponsabilites()
updater.tearDown()
```


