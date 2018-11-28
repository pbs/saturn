import boto3
import time
import json


class StopPagination(Exception):
    """ used to break out of pagination loops """


def format_seconds(seconds):
    seconds = int(seconds)
    elapsed = f"{seconds//3600}:{(seconds%3600//60):02d}:{seconds%3600%60:02d}"
    return elapsed


def get_target_for_rule(name):
    events = boto3.client("events")
    targets = events.list_targets_by_rule(Rule=name)["Targets"]
    if len(targets) != 1:
        raise NotImplementedError("too many targets")
    return targets[0]


def get_rules_by_prefix(prefix):
    events = boto3.client("events")
    rules = events.list_rules(NamePrefix=prefix)["Rules"]
    results = []
    for rule in rules:
        target = get_target_for_rule(rule["Name"])
        results.append(
            {
                "name": rule["Name"],
                "schedule": rule["ScheduleExpression"],
                "enabled": rule["State"] == "ENABLED",
                "target_ecs_parameters": target["EcsParameters"],
                "target_arn": target["Arn"],
                "target_id": target["Id"],
                "target_input": target["Input"],
                "target_role_arn": target["RoleArn"],
                "target_command": " ".join(
                    json.loads(target["Input"])["containerOverrides"][0]["command"]
                ),
            }
        )
    return results


def get_logs_for_rule(rule_name, n):
    ecs = boto3.client("ecs")
    logs = boto3.client("logs")

    target = get_target_for_rule(rule_name)
    task_def = ecs.describe_task_definition(
        taskDefinition=target["EcsParameters"]["TaskDefinitionArn"]
    )["taskDefinition"]
    if len(task_def["containerDefinitions"]) > 1:
        raise NotImplementedError("too many containers")

    # figure out the group & prefix from the underlying ECS config
    log_config = task_def["containerDefinitions"][0]["logConfiguration"]
    log_group = log_config["options"]["awslogs-group"]
    log_prefix = log_config["options"]["awslogs-stream-prefix"]
    name = task_def["containerDefinitions"][0]["name"]

    prefix = f"{log_prefix}/{name}/"
    paginator = logs.get_paginator("describe_log_streams")
    # if we pass logStreamNamePrefix, we can't order by date(?!) so filter manually
    response_iterator = paginator.paginate(
        logGroupName=log_group, orderBy="LastEventTime", descending=True
    )

    log_streams = []
    try:
        for page in response_iterator:
            for ls in page["logStreams"]:
                if ls["logStreamName"].startswith(prefix):
                    log_streams.append(ls)
                    if len(log_streams) >= n:
                        raise StopPagination()
    except StopPagination:
        pass

    return log_group, log_streams


def get_log_for_run(log_group_name, log_stream_name, num_lines):
    logs = boto3.client("logs")
    extra = {}

    paginator = logs.get_paginator("filter_log_events")
    response_iterator = paginator.paginate(
        logGroupName=log_group_name, logStreamNames=[log_stream_name]
    )

    return [event for page in response_iterator for event in page["events"]][-num_lines:]


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
