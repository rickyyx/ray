import click
from rich import print
from rich.panel import Panel

from ray.experimental.hack.lib import StuckTasks, IdleNodes


def print_results(results):
    oks = [r for r in results if r.is_ok()]
    issues = [r for r in results if not r.is_ok()]

    issues = sorted(issues, key=lambda r: r.level)
    for issue in issues:
        print(f"{issue.details}")
        panel_txt = ""
        for action in issue.actions:
            panel_txt += f"{action}"
        print(Panel(panel_txt))
    

@click.command()
def ray_doctor():
    checks = [StuckTasks(), IdleNodes()]
    results = []
    for check in checks:
        results.append(check.to_diagnosis())

    print_results(results)
