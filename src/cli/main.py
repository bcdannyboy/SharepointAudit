"""Main entry point for the SharePoint Audit CLI."""

import click

from .commands import audit, dashboard, backup, restore, health


@click.group()
@click.version_option(version="1.0.0", prog_name="sharepoint-audit")
@click.pass_context
def cli(ctx):
    """SharePoint Audit Utility - Comprehensive SharePoint Online auditing tool.

    This CLI provides commands to audit SharePoint sites, analyze permissions,
    view dashboards, and manage audit data.
    """
    # Ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the usual CLI invocation)
    ctx.ensure_object(dict)


# Add commands to the main group
cli.add_command(audit)
cli.add_command(dashboard)
cli.add_command(backup)
cli.add_command(restore)
cli.add_command(health)


def main():
    """Main entry point function."""
    cli(prog_name="sharepoint-audit")


if __name__ == "__main__":
    main()
