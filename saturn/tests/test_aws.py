import boto3
import json
from moto import mock_events
from saturn.utils import get_rules_by_prefix


def build_fake_env():
    events = boto3.client("events")

    to_create = [
        {"name": "example-a", "target_id": "aaa", "target_arn": "arn:a", "command": "./a"},
        {
            "name": "example-b",
            "target_id": "bbb",
            "target_arn": "arn:b",
            "command": ["./b", "--flag", "2"],
            "state": "DISABLED",
        },
        {"name": "example-c", "target_id": "bbb", "target_arn": "arn:c", "command": "./c"},
        {"name": "other-x", "target_id": "xxx", "target_arn": "arn:x", "command": "./x"},
    ]

    for e in to_create:
        events.put_rule(
            Name=e["name"], ScheduleExpression="rate(10 minutes)", State=e.get("state", "ENABLED")
        )
        events.put_targets(
            Rule=e["name"],
            Targets=[
                {
                    "Input": json.dumps({"containerOverrides": [{"command": e["command"]}]}),
                    "Id": e["target_id"],
                    "Arn": e["target_arn"],
                    "EcsParameters": {
                        "TaskDefinitionArn": e["target_arn"],
                        "LaunchType": "FARGATE",
                    },
                }
            ],
        )


@mock_events
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
