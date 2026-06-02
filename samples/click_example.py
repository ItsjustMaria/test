import click


@click.group()
def cli():
    pass


def download_logic():

    data = "customers.csv"
    log = "download.log"
    error = "No critical errors"

    return data, log, error


def process_logic():

    click.echo("Processing started...")


@cli.command()
def pipeline():

    data, log, error = download_logic()

    print(f"""
Output of the first step:

Data:
{data}

Log created:
{log}

Errors:
{error}
""")
################## TWO OPTIONS FOR STEP BY STEP LOGIC ##############

    ########## OPTION 1 ###############
    proceed = click.confirm("Do you wish to proceed?")

    if proceed:
        process_logic()

    else:
        click.echo("Pipeline stopped.")

    ########## OPTION 2 cleaner ###############
    click.echo("Step 1 done")

    click.confirm("Continue?", abort=True)

    click.echo("Step 2 running")


if __name__ == "__main__":
    cli()