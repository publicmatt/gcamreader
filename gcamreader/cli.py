from pathlib import Path
from .querymi import LocalDBConn, RemoteDBConn, parse_batch_query
import click
from click_default_group import DefaultGroup
import sys


@click.group(
    cls=DefaultGroup,
    default="local",
    default_if_no_args=True,
)
@click.version_option()
def cli():
    """
    Run queries against a gcam scenario database.

        Saves outputs as .csv

    Documentation: https://github.com/JGCRI/gcamreader/
    """


@cli.command(name="local")
@click.option(
    "-d",
    "--database_path",
    type=click.Path(exists=True, file_okay=False, readable=True, path_type=Path),
    required=True,
    help="path to database file (i.e. parent of *.basex dir)",
)
@click.option(
    "-q",
    "--query_path",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    required=True,
    help="path to xml with queries to run (i.e: Main_queries.xml)",
)
@click.option(
    "-o",
    "--output_path",
    type=click.Path(exists=True, file_okay=False, writable=True, path_type=Path),
    help="path to output (i.e. where .csv files should be created)",
)
def local(database_path: Path, query_path: Path, output_path: Path):
    """
    query gcam scenario databases
    """

    print(f"opening: {database_path.absolute()}", file=sys.stderr)
    if not list(database_path.glob("*.basex")):
        print(f"basex files missing: {database_path}", file=sys.stderr)
        return False
    parent = str(database_path.parent)
    name = database_path.name

    # establish database connection - uses ModelInterface.jar
    conn = LocalDBConn(parent, name)

    # parse query xml
    print(f"parsing: {query_path.name}", file=sys.stderr)
    queries = parse_batch_query(str(query_path))
    for query in queries:
        print(f"running: {query.title}", file=sys.stderr)
        df = conn.runQuery(query)
        if df is None:
            print(f"failed: {query.title}")
            continue
        out = output_path / f"{str(query.title).replace(' ', '_').lower()}.csv"
        df.to_csv(out, index=False, sep="|")
        print(f"saved: {out.absolute()}", file=sys.stderr)
    print(f"extract complete", file=sys.stderr)


@cli.command(name="remote")
@click.option(
    "-u",
    "--username",
    type=str,
    required=True,
    help="username of remote server authentication",
)
@click.option(
    "-w",
    "--password",
    type=str,
    prompt=True,
    hide_input=True,
    help="password of remote server authentication",
)
@click.option(
    "-n",
    "--hostname",
    type=str,
    default="localhost",
    required=True,
    help="hostname of remote server",
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=8984,
    required=True,
    help="hostname of remote server",
)
@click.option(
    "-d",
    "--database_name",
    type=str,
    required=True,
    help="name of database to query (i.e. parent of *.basex dir)",
)
@click.option(
    "-q",
    "--query_path",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    required=True,
    help="path to xml with queries to run (i.e: Main_queries.xml)",
)
@click.option(
    "-o",
    "--output_path",
    type=click.Path(exists=True, file_okay=False, writable=True, path_type=Path),
    help="path to output (i.e. where .csv files should be created)",
)
def remote(
    username: str,
    password: str,
    hostname: str,
    port: int,
    database_name: str,
    query_path: Path,
    output_path: Path,
):
    """
    query a remote server containing gcam scenario databases
    """

    # establish database connection - uses ModelInterface.jar
    conn = RemoteDBConn(
        username=username,
        password=password,
        address=hostname,
        port=port,
        dbfile=database_name,
    )

    # parse query xml
    print(f"parsing: {query_path.name}", file=sys.stderr)
    queries = parse_batch_query(str(query_path))
    for query in queries:
        print(f"running: {query.title}", file=sys.stderr)
        df = conn.runQuery(query)
        if df is None:
            print(f"failed: {query.title}")
            continue
        out = output_path / f"{str(query.title).replace(' ', '_').lower()}.csv"
        df.to_csv(out, index=False, sep="|")
        print(f"saved: {out.absolute()}", file=sys.stderr)
    print(f"extract complete", file=sys.stderr)
