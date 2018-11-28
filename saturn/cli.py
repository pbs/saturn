import sys
import time
import click
from tabulate import tabulate
from .utils import (
    get_rules_by_prefix,
    get_runs_for_rule,
    get_log_for_run,
    format_seconds,
    run_task,
)

HASH_LENGTH = 6


@click.group()
def cli():
    pass


@click.command()
@click.argument("prefix", default="")
def tasks(prefix):
    """
        List currently scheduled tasks.
    """
    rules = get_rules_by_prefix(prefix)

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
@click.option("-n", default=5, help="number of runs to show")
@click.option("--detailed/--no-detailed", help="get detailed information on the runs")
def runs(job_name, n, detailed):
    """
        List most recent N runs of JOB_NAME.

        Runs are referred to by hash ids.
    """
    log_group, runs = get_runs_for_rule(job_name, n, detailed)
    display_runs = []
    for log in runs:
        first = log["firstEventTimestamp"]
        last = log["lastEventTimestamp"]
        elapsed = format_seconds((last - first) / 1000)
        cols = [
            click.style(log["logStreamName"][-HASH_LENGTH:], bold=True),
            time.ctime(last / 1000),
            elapsed,
        ]
        if detailed:
            cols += [log["status"], log["exit_code"]]
        display_runs.append(cols)

    headers = ["run id", "last event", "elapsed"]
    if detailed:
        headers += ["status", "exit code"]

    click.echo(tabulate(display_runs, headers=headers))


@click.command()
@click.argument("job-name")
@click.argument("log-id", default="latest")
@click.option("-n", default=50, help="number of lines to print")
@click.option("--watch/--no-watch", help="watch log until user breaks")
@click.option("--timestamp/--no-timestamp", help="add timestamp")
def logs(job_name, log_id, n, watch, timestamp):
    """
        Show logs for specific run.

        If LOG_ID is provided, will show a specific log, otherwise the latest log
        will be displayed.
    """
    log_group, runs = get_runs_for_rule(job_name, 10)
    if log_id == "latest":
        run = runs[0]
    else:
        for run in runs:
            if run["logStreamName"].endswith(log_id):
                break
        else:
            click.secho("no such run", fg="red")
            sys.exit(1)

    for line in get_log_for_run(log_group, run["logStreamName"], n, watch):
        if timestamp:
            click.secho(time.ctime(line["timestamp"] / 1000) + " ", fg="blue", nl=False)
        click.echo(line["message"])


@click.command()
@click.argument("job-name")
def run(job_name):
    """
        Kick off a run of a task.

        This will use the same settings as configured in the scheduled task.
    """
    cluster_id, task_id = run_task(job_name)
    click.secho(f"started run {task_id[-HASH_LENGTH:]}", fg="green")


cli.add_command(tasks)
cli.add_command(runs)
cli.add_command(logs)
cli.add_command(run)

if __name__ == "__main__":
    cli()
