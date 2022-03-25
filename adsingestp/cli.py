import click

# try:
#     import rutils
#
#     config = rutils.load_config()
#     logger = rutils.setup_logging("adsingestp.cli")
# except ImportError:
#     import logging
#
#     config = {}
#     logger = logging.getLogger("adsingestp.cli")


@click.group()
def cli():
    pass


@cli.command()
def hello():
    """Will greet"""
    print("Hello World!")


if __name__ == "__main__":
    cli()
