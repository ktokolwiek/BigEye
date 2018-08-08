from ruamel.yaml import YAML
from argparse import ArgumentParser
import logging
from os import environ
import boto3
from base64 import b64decode


class Config():
    """Parses config at given path completes prod config with encrypted environment variables

    :param relativePath: relative path to the config yaml file
    :type relativePath: string
    :param env: environment 'prod' or 'dev', if run locally use dev, defaults to 'prod'
    :param env: str, optional
    :param role: role, similar as roles in BigEye class, defaults to 'slave'
    :param role: str, optional
    """

    def __init__(self, relativePath, env='prod', role='slave'):
        if '.yaml' not in relativePath:
            raise Exception(
                'Provided path to config file is not an .yaml formated file')
        yaml = YAML()
        with open(relativePath) as f:
            self.config = yaml.load(f)
        if env == 'prod':
            # if it is a production run, passwords and keys are not the config file but stored in encrypted environment variables
            kmsClient = boto3.client('kms')
            try:
                if role == 'slave':
                    for fet in self.config['Fetchers']:
                        self.config['Fetchers'][fet]['password'] = self.getPasswordsFromKMS(
                            kmsClient, fet+'_password')
                self.config['Publishers']['bboxx_dd']['apiKey'] = self.getPasswordsFromKMS(
                    kmsClient, 'bboxx_dd_apiKey')
                self.config['Publishers']['bboxx_dd']['appKey'] = self.getPasswordsFromKMS(
                    kmsClient, 'bboxx_dd_appKey')
            except KeyError as err:
                raise Exception(
                    'Key not in environment variables, {}'.format(err))

    def getPasswordsFromKMS(self, client, KMSKey):
        """Gets encrypted passwords from envrionment variables and decrypts it

        :param client: boto3 kms client
        :type client: boto3 client
        :param KMSKey: name of the environment variable
        :type KMSKey: string
        :return: plain text string of variable
        :rtype: string
        """

        plainBytes = client.decrypt(
            CiphertextBlob=b64decode(environ[KMSKey]))['Plaintext']
        plainStringValue = plainBytes.decode()
        return plainStringValue

    def getValue(self, *args):
        """Returns value from config following sequence of keys given in args

        :raises err: if the sequence of keys is not in the config
        :return: value stored in the config
        :rtype: string
        """

        try:
            value = self.config
            for arg in args:
                value = value[arg]
        except KeyError as err:
            print(
                'The supplied sequence of keys is not in the config or in the environment variables')
            raise err
        return value


class CLIArgsParser():
    """
    Parses cli arguments for main and sub functionalities of the repo
    """

    def __init__(self):
        self.parser = ArgumentParser(
            description='Hunts down mischievous data', epilog='Hope the tool answers some of your monitoring needs ;)')
        # defaults to false
        self.parser.add_argument(
            'role', help='specify the mode you wish to use', choices=['master', 'slave', 'updateBoards', 'updateLambda', 'invokeMaster', 'invokeSlave'])

    def parseArgs(self):
        """Parses the args of the command line

        :return: args
        :rtype: namespace
        """

        args = self.parser.parse_args()
        return args


class LogHandler():
    """
    Handles logs from runtime
    """

    @staticmethod
    def createLogHandler(env='prod'):
        """Creates log handler, without handler is env is prod with handlers otherwise

        :param env: runtime environment, put 'dev' if run locally, defaults to 'prod'
        :param env: str, optional
        :return: logger instance from logging module
        :rtype: logger
        """

        logger = logging.getLogger('main')
        logger.setLevel(logging.INFO)
        # Checks if logger has not been initialised before to avoid adding excessive handlers which duplicate messages
        if env != 'prod' and not logger.hasHandlers():
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            logger.addHandler(ch)
        return logger
