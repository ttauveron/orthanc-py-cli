import json
import random
from datetime import date

import click
from beren import Orthanc
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from auth.bearer import BearerAuth
from utils.upload import OrthancUpload


@click.group()
@click.option('--server', envvar='SERVER', help='Orthanc server url')
@click.option('--iap-client-id', envvar='IAP_CLIENT_ID', help='Google Identity-Aware Proxy client ID')
def cli(server, iap_client_id):
    open_id_connect_token = id_token.fetch_id_token(Request(), iap_client_id)
    global orthanc
    orthanc = Orthanc(server=server, auth=BearerAuth(open_id_connect_token))
    pass


@click.command()
def patients():
    """List patients in Orthanc"""
    patients = orthanc.get_patients(expand=True)
    click.echo(json.dumps(patients, indent=2, sort_keys=True))


@click.command()
def studies():
    """List studies in Orthanc"""
    studies = orthanc.get_studies(expand=True)
    click.echo(json.dumps(studies, indent=2, sort_keys=True))


@click.command()
def instances():
    """List DICOM instances in Orthanc"""
    instances = orthanc.get_instances(expand=True)
    click.echo(json.dumps(instances, indent=2, sort_keys=True))


@click.command()
@click.argument('files', nargs=-1)
def upload(files):
    """Upload files or directories containing DICOM instances (bz2,gz,tar,zip supported)"""
    OrthancUpload(orthanc).upload(files)


@click.command()
@click.argument('study_id', nargs=1)
def download(study_id):
    """Download a study into a zip archive"""
    archive = orthanc.get_study_archive(study_id)
    with open("{}.zip".format(study_id), "wb") as f:
        for chunk in archive:
            f.write(chunk)


@click.command()
@click.argument('study_id', nargs=1)
def anonymize(study_id):
    """Anonymize a study and set DICOM tags for veye reprocessing"""
    data = {
        "Replace": {
            "AccessionNumber": str(random.randint(1000000, 9999999)),
            "StudyDate": str(date.today().strftime("%Y%m%d"))
        },
        "Asynchronous": True
    }
    orthanc.anonymize_study(study_id, data)


cli.add_command(patients)
cli.add_command(upload)
cli.add_command(download)
cli.add_command(studies)
cli.add_command(instances)
cli.add_command(anonymize)

if __name__ == '__main__':
    cli()
