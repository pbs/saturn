import boto3
import time
import json


def get_rules_by_prefix(prefix):
    events = boto3.client("events")
    rules = events.list_rules(NamePrefix=prefix)["Rules"]
    results = []
    for rule in rules:
        targets = events.list_targets_by_rule(Rule=rule["Name"])["Targets"]
        if len(targets) != 1:
            raise NotImplementedError("too many targets")
        # unused: Arn, Description,
        results.append(
            {
                "name": rule["Name"],
                "schedule": rule["ScheduleExpression"],
                "enabled": rule["State"] == "ENABLED",
                "target_ecs_parameters": targets[0]["EcsParameters"],
                "target_arn": targets[0]["Arn"],
                "target_id": targets[0]["Id"],
                "target_input": targets[0]["Input"],
                "target_role_arn": targets[0]["RoleArn"],
                "target_command": " ".join(
                    json.loads(targets[0]["Input"])["containerOverrides"][0]["command"]
                ),
            }
        )
    return results


def get_rule_by_name(job_name):
    # TODO: optimize this, skip the list and go straight to rule
    rules = get_rules_by_prefix("update-")
    for rule in rules:
        if rule["name"] == job_name:
            return rule


def get_logs_for_rule(rule):
    ecs = boto3.client("ecs")

    task_def = ecs.describe_task_definition(
        taskDefinition=rule["target_ecs_parameters"]["TaskDefinitionArn"]
    )["taskDefinition"]
    if len(task_def["containerDefinitions"]) > 1:
        raise NotImplementedError("too many containers")

    # figure out the group & prefix from the underlying ECS config
    log_config = task_def["containerDefinitions"][0]["logConfiguration"]
    log_group = log_config["options"]["awslogs-group"]
    log_prefix = log_config["options"]["awslogs-stream-prefix"]
    name = task_def["containerDefinitions"][0]["name"]
    return log_group, get_log_streams(log_group, f"{log_prefix}/{name}/")


def get_log_streams(log_group, prefix=""):
    logs = boto3.client("logs")
    # if we pass logStreamNamePrefix, we can't order by date (?!) so
    # we'll just filter manually
    response = logs.describe_log_streams(
        logGroupName=log_group, orderBy="LastEventTime", descending=True
    )
    return [ls for ls in response["logStreams"] if ls["logStreamName"].startswith(prefix)]


def get_log_for_run(log_group_name, log_stream_name):
    logs = boto3.client("logs")
    extra = {}

    while True:
        try:
            events = logs.get_log_events(
                logGroupName=log_group_name, logStreamName=log_stream_name, **extra
            )
        except ClientError:
            yield {"message": "no logs"}
            break

        if not events["events"]:
            break

        yield from events["events"]

        if events["nextForwardToken"]:
            extra = {"nextToken": events["nextForwardToken"]}
        else:
            break


def run_task(rule):
    ecs = boto3.client("ecs")

    task_def = ecs.describe_task_definition(
        taskDefinition=rule["target_ecs_parameters"]["TaskDefinitionArn"]
    )["taskDefinition"]

    # AWS is annoyingly inconsistent w/ case here
    mutated_network_config = {}
    for key, val in rule["target_ecs_parameters"]["NetworkConfiguration"][
        "awsvpcConfiguration"
    ].items():
        new_key = key[:1].lower() + key[1:]
        mutated_network_config[new_key] = val

    response = ecs.run_task(
        cluster=rule["target_arn"],
        startedBy="saturn",  # TODO: fix this
        taskDefinition=task_def["taskDefinitionArn"],
        count=rule["target_ecs_parameters"]["TaskCount"],
        launchType=rule["target_ecs_parameters"]["LaunchType"],
        networkConfiguration={"awsvpcConfiguration": mutated_network_config},
        # does this need to specify platformVersion?
        overrides=json.loads(rule["target_input"]),
    )

    if len(response["tasks"]) == 1 and len(response["failures"]) == 0:
        return response["tasks"][0]["taskArn"]
    else:
        print("ERROR:", response["failures"])
