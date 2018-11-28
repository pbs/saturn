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


@cli.add_command
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


@cli.add_command
@click.command()
@click.argument("job-name")
@click.option("-n", default=5, help="number of runs to show")
@click.option("--detailed/--no-detailed", help="get detailed information on the runs")
def runs(job_name, n, detailed):
    """
        List most recent N runs of JOB_NAME.

        Runs are referred to by hash ids.
    """
    log_group, runs = get_runs_for_rule(job_name, n, detailed=detailed)
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


@cli.add_command
@click.command()
@click.argument("job-name")
@click.argument("run-id", default="latest")
@click.option("-n", default=50, help="number of lines to print")
@click.option("--watch/--no-watch", help="watch log until user breaks")
@click.option("--timestamp/--no-timestamp", help="add timestamp")
def logs(job_name, run_id, n, watch, timestamp):
    """
        Show logs for specific run.

        If RUN_ID is provided, will show a specific run, otherwise the latest run
        will be displayed.
    """
    if run_id == "latest":
        log_group, runs = get_runs_for_rule(job_name, n=1)
        run = runs[0]
    else:
        log_group, runs = get_runs_for_rule(job_name, run_id=run_id)
        if not runs:
            click.secho("no such run", fg="red")
            sys.exit(1)
        run = runs[0]

    for line in get_log_for_run(log_group, run["logStreamName"], n, watch):
        if timestamp:
            click.secho(time.ctime(line["timestamp"] / 1000) + " ", fg="blue", nl=False)
        click.echo(line["message"])


@cli.add_command
@click.command()
@click.argument("job-name")
@click.option("--watch/--no-watch", help="watch log until user breaks")
@click.pass_context
def start(ctx, job_name, watch):
    """
        Kick off a run of a task.

        This will use the same settings as configured in the scheduled task.
    """
    cluster_id, task_id = run_task(job_name)
    click.secho(f"started run {task_id[-HASH_LENGTH:]}", fg="green")

    if watch:
        run_id = task_id.split("/")[-1]
        run = None

        # wait for the logs to start
        click.echo("waiting for logs to appear", nl=False)
        time.sleep(30)
        while True:
            log_group, runs = get_runs_for_rule(job_name, run_id=run_id)
            if runs:
                run = runs[0]
                break
            click.echo(".", nl=False)
            time.sleep(5)

        click.secho("done!", fg="green")

        # ok the logs are ready
        for line in get_log_for_run(log_group, run["logStreamName"], 100, watch):
            click.echo(line["message"])


if __name__ == "__main__":
    cli()
