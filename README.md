# saturn

a tool for working with scheduled tasks on ECS

## Setting Up Your ECS Environment

This tool does not take responsibility for:

* scheduling jobs 
* complex jobs with multiple containers

It is recommended that you use [Terraform](https://www.terraform.io/) for scheduling jobs and maintianing ECS clusters, and adhere to the following guidelines:

1) Group similar jobs by prefix.  Due to the way ECS works, this will make it much easier to deal with the subset of jobs that is relevant to you.  A recommended scheme: `project-env-taskname`.

2) If you have multiple tasks that use the same container, only create one task and use the containerOverrides to set different commands on each task.  (This helps with not hitting ECS quotas as well as getting better visibility from this tool into what your tasks are doing.)

## Using Saturn

Saturn can be used to interact with already scheduled ECS tasks in several ways:

### Listing scheduled jobs

```
$ saturn tasks
update-nolas-prod    ✓  cron(0 7 * * ? *)        django-admin update_nolas
update-nolas-qa      ✓  cron(0 7 * * ? *)        django-admin update_nolas
update-ota-prod      ✓  cron(0 6 ? * MON-FRI *)  django-admin update_lineups --ota
update-ota-qa        ✓  cron(0 6 ? * MON-FRI *)  django-admin update_lineups --ota
```


### Viewing past runs of scheduled jobs

```
$ saturn runs update-sources-qa
run id    last event                elapsed
--------  ------------------------  ---------
34a9f0    Tue Nov 27 23:02:07 2018  0:00:01
2c072e    Tue Nov 27 23:01:39 2018  0:00:01
155b02    Tue Nov 27 22:56:12 2018  0:00:02
ac97a1    Tue Nov 27 01:01:48 2018  0:00:03
b4a90a    Mon Nov 26 01:02:31 2018  0:00:47
```

### Viewing log output from previous runs

```
$ saturn logs update-sources-qa ac97a1
```

### Starting one-off runs of existing jobs

```
$ saturn run update-sources-qa
```
