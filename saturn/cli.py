import time
import click
from tabulate import tabulate
from .utils import get_rules_by_prefix, get_logs_for_rule, get_rule_by_name, get_log_for_run


@click.group()
def cli():
    pass


@click.command()
def list():
    # TODO: make prefix configurable
    rules = get_rules_by_prefix("update-")

    display_rules = []

    for rule in rules:
        ecolor = "green" if rule["enabled"] else "red"
        display_rules.append(
            [
                click.style(rule["name"], bold=True),
                click.style("✓", fg=ecolor) if rule["enabled"] else click.style("✗", fg=ecolor),
                click.style(rule["schedule"], fg=ecolor),
                click.style(rule["target_command"], fg="blue"),
            ]
        )
    click.echo(tabulate(display_rules, tablefmt="plain"))


@click.command()
@click.argument("job-name")
def runs(job_name):
    # TODO: make number of logs and display characters of hash configurable
    rule = get_rule_by_name(job_name)
    log_group, runs = get_logs_for_rule(rule)
    for log in runs:
        print(
            click.style(log["logStreamName"][-4:], bold=True),
            time.ctime(log["firstEventTimestamp"] / 1000),
            time.ctime(log["lastEventTimestamp"] / 1000),
        )


@click.command()
@click.argument("job-name")
@click.argument("log-id", default="latest")
def logs(job_name, log_id):
    rule = get_rule_by_name(job_name)
    log_group, runs = get_logs_for_rule(rule)
    if log_id == "latest":
        run = runs[0]
    else:
        for run in runs:
            if run["logStreamName"].endswith(default):
                break
        else:
            print("no such run")

    for line in get_log_for_run(log_group, run["logStreamName"]):
        print(click.style(time.ctime(line["timestamp"] / 1000), fg="blue"), line["message"])


cli.add_command(list)
cli.add_command(runs)
cli.add_command(logs)

if __name__ == "__main__":
    cli()
