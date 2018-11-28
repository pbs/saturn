import boto3
import json
from moto import mock_events, mock_ecs, mock_logs
from saturn.utils import get_rules_by_prefix, get_runs_for_rule


def build_fake_env():
    events = boto3.client("events")
    ecs = boto3.client("ecs")
    logs = boto3.client("logs")

    to_create = [
        {
            "name": "example-a",
            "target_id": "aaa",
            "target_arn": "example-task:1",
            "command": "./a",
            "runs": ["1"],
        },
        {
            "name": "example-b",
            "target_id": "bbb",
            "target_arn": "example-task:1",
            "command": ["./b", "--flag", "2"],
            "state": "DISABLED",
            "runs": ["2", "3"],
        },
        {
            "name": "example-c",
            "target_id": "bbb",
            "target_arn": "example-task:1",
            "command": "./c",
            "runs": ["4", "5", "6"],
        },
        {"name": "other-x", "target_id": "xxx", "target_arn": "other-task:1", "command": "./x",
         "runs": [] },
    ]

    logs.create_log_group(logGroupName="ecs/logs")

    ecs.create_cluster(clusterName="cluster-arn")

    ecs.register_task_definition(
        family="example-task",
        taskRoleArn="example-task:1",
        executionRoleArn="arn:ecs-role",
        containerDefinitions=[
            {
                "name": "example",
                "image": "docker-image",
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {"awslogs-group": "ecs/logs", "awslogs-stream-prefix": "ecs/logs"},
                },
            }
        ],
    )

    for e in to_create:
        events.put_rule(
            Name=e["name"], ScheduleExpression="rate(10 minutes)", State=e.get("state", "ENABLED")
        )
        events.put_targets(
            Rule=e["name"],
            Targets=[
                {
                    "Input": json.dumps({"containerOverrides": [{"name": e["name"],
                                                                 "command": e["command"]}]}),
                    "Id": e["target_id"],
                    "Arn": "cluster-arn",
                    "EcsParameters": {
                        "TaskDefinitionArn": e["target_arn"],
                        "LaunchType": "FARGATE",
                    },
                }
            ],
        )
        for run in e["runs"]:
            logs.create_log_stream(logGroupName="ecs/logs",
                                   logStreamName=f"ecs/logs/{e['name']}/{run}")


@mock_ecs
@mock_events
@mock_logs
def test_get_rules_by_prefix():
    build_fake_env()
    rules = get_rules_by_prefix(None)
    assert len(rules) == 4
    rules = get_rules_by_prefix("example")
    assert len(rules) == 3
    assert rules[0]["name"] == "example-a"
    assert "10 minutes" in rules[0]["schedule"]
    assert rules[0]["enabled"] is True
    assert rules[1]["enabled"] is False
    assert rules[1]["target_command"] == "./b --flag 2"


@mock_ecs
@mock_events
@mock_logs
def test_get_runs_for_rule_basic():
    build_fake_env()
    log_group, runs = get_runs_for_rule("example-a")
    assert len(runs) == 1

    log_group, runs = get_runs_for_rule("example-b")
    assert len(runs) == 2


@mock_ecs
@mock_events
@mock_logs
def test_get_runs_for_rule_n_param():
    build_fake_env()
    log_group, runs = get_runs_for_rule("example-b", n=100)
    assert len(runs) == 2
    log_group, runs = get_runs_for_rule("example-b", n=1)
    assert len(runs) == 1


@mock_ecs
@mock_events
@mock_logs
def test_get_runs_for_rule_run_id():
    build_fake_env()
    log_group, runs = get_runs_for_rule("example-b", run_id="3")
    assert len(runs) == 1
    log_group, runs = get_runs_for_rule("example-b", run_id="x")
    assert len(runs) == 0


# run_task doesn't seem to be mocked properly yet
# @mock_ecs
# @mock_events
# @mock_logs
# def test_get_runs_for_rule_detailed():
#     build_fake_env()
#     log_group, runs = get_runs_for_rule("example-b", detailed=True)
#     assert len(runs) == 2
#     assert runs[0]["status"] == "RUNNING"
#     assert runs[0]["exit_code"] == 0
