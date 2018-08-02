import boto3
import json
import zipfile
from glob import glob
import os
from io import BytesIO
from .config import Config, CLIArgsParser, LogHandler


class LambdaClient:
    """
    Lambda client to create, update and invoke lambda functions
    """

    def __init__(self, config, logger, env):
        self.config = config
        self.logger = logger
        if env == 'prod':
            # the lambda function has appropriate policy to invoke lambda, no keys needed
            self.client = boto3.client('lambda')
        else:
            # need keys for running locally
            self.client = boto3.client('lambda', aws_access_key_id=config.getValue('aws', 'publicKey'),
                                       aws_secret_access_key=config.getValue('aws', 'secretKey'))

    def createFunction(self, name, zipContent):
        print('Uploading function to aws')
        resp = self.client.create_function(FunctionName=name, Runtime='python3.6',
                                           Role='arn:aws:iam::131232499809:role/lambda-data-police',
                                           Handler='main.lambdaEntry',
                                           Code={'ZipFile': zipContent},
                                           Timeout=300)
        print(resp)

    def updateFunction(self, name, zipContent):
        print('Updating function...')
        resp = self.client.update_function_code(FunctionName=name,
                                                ZipFile=zipContent)
        print(resp)

    def invokeFunction(self, functionName, mode, event):
        """
        Invokes function synchronously or asynchronously depending on mode with event as input
        """
        if mode not in ('sync', 'async'):
            raise Exception(
                'Mode for calling lambda function should be sync or async')
        modeInvocationMapping = {'sync': 'RequestResponse', 'async': 'Event'}
        resp = self.client.invoke(FunctionName=functionName,
                                  InvocationType=modeInvocationMapping[mode],
                                  Payload=event)
        self.logger.info(resp)


class Zipper:

    def __init__(self, filesToInclude, pathToEnv, destinationPath=None, packagesToExclude=[], toBuffer=True):
        """
        Path to the site-packages directory, for example ../dp-env/lib/python3.6/site-packages/
        Packages to exclude such as linters
        Where the script will go to look for files to add to the zip package to add to the root of zip
        Destination path where the zip folder should be created
        """
        self.zip = zipfile.ZipFile(destinationPath, 'w', zipfile.ZIP_DEFLATED)
        self.filesToInclude = filesToInclude
        self.pathToEnv = pathToEnv
        self.packagesToExclude = packagesToExclude
        self.destinationPath = destinationPath
        self.toBuffer = toBuffer
        if toBuffer:
            self.buf = BytesIO()
            self.zip = zipfile.ZipFile(self.buf, 'w')

    def addFilesFromLookUpPaths(self):
        """
        Looks for the files that correspond the format specified in the LOOKUP_PATHS list
        and adds them to the root of the zip file
        """
        filesToAdd = [filePath for lookupPath in self.filesToInclude
                      for filePath in glob(os.path.join(lookupPath), recursive=True)]
        for fileToAdd in filesToAdd:
            # set destination path to root of zip file by removing extra dot
            self.zip.write(fileToAdd, fileToAdd[1:])
        print('Added {} files found from the look up paths list to the zip package'.format(
            len(filesToAdd)))

    def addLibrariesToZip(self):
        """
        Adds all packages in the PATH_TO_VIRTUAL_ENV_PACKAGES to the zip file
        """
        for packageToAdd in glob(os.path.join(self.pathToEnv, '**/*'), recursive=True):
            # removes excess subdirectories as required by aws
            destinationPath = os.path.join(*packageToAdd.split('/')[5:])
            packageName = destinationPath.split('/')[0]
            if packageName not in self.packagesToExclude:
                self.zip.write(packageToAdd, destinationPath)
        print('Added the packages files from the virtual env path: {}'.format(
            self.pathToEnv))

    def addExternalLibraries(self):
        """
        Adds external psycopg2 package with static C library as AMI image does not have it
        """
        for packageToAdd in glob('../../awslambda-psycopg2/psycopg2-3.6/**/*', recursive=True):
            destinationPath = os.path.join(
                'psycopg2', *packageToAdd.split('/')[4:])
            self.zip.write(packageToAdd, destinationPath)
        print('Added external psycopg2')

    def buildZip(self):
        """
        Builds the zip
        """
        self.addFilesFromLookUpPaths()
        self.addLibrariesToZip()
        self.addExternalLibraries()
        # return the zip if mode is toBuffer
        if self.toBuffer:
            self.buf.seek(0)
            self.zip.close()
            return self.buf.read()
        self.zip.close()
