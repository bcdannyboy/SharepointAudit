"""Main entry point for the SharePoint Audit CLI."""

import click

# Import dashboard separately to avoid import issues
from cli.dashboard_command import dashboard

# Import commands
try:
    from cli.commands import audit, backup, restore, health
    from cli.recovery_command import recovery_status
    from cli.run_info_command import run_info
except ImportError as e:
    # If there's an import error, use the simple fallback
    import traceback
    print(f"Warning: Unable to load full commands module: {e}")
    print("Traceback:")
    traceback.print_exc()

    # Fallback to simple audit implementation
    from cli.simple_audit import audit

    # Create fallback commands
    @click.command()
    @click.pass_context
    def backup(ctx):
        click.echo("Error: Unable to load backup command due to import issues.")
        ctx.exit(1)

    @click.command()
    @click.pass_context
    def restore(ctx):
        click.echo("Error: Unable to load restore command due to import issues.")
        ctx.exit(1)

    @click.command()
    @click.pass_context
    def health(ctx):
        click.echo("Error: Unable to load health command due to import issues.")
        ctx.exit(1)

    @click.command()
    @click.pass_context
    def recovery_status(ctx):
        click.echo("Error: Unable to load recovery status command due to import issues.")
        ctx.exit(1)

    @click.command()
    @click.pass_context
    def run_info(ctx):
        click.echo("Error: Unable to load run info command due to import issues.")
        ctx.exit(1)




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
cli.add_command(recovery_status)
cli.add_command(run_info)


def main():
    """Main entry point function."""
    cli(prog_name="sharepoint-audit")


if __name__ == "__main__":
    main()
