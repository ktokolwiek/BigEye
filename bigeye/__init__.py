from time import sleep
from json import dumps
from .config import Config, LogHandler, CLIArgsParser
from .awsldaClient import LambdaClient, Zipper
from .tests import TestManager
from .fetchers import FetcherManager
from .publishers import PublisherManager


class BigEye:

    def __init__(self, env, role, configPath, testsPath, extraParameters={}):
        """Initialises a big eye instance

        :param env: environment either 'dev' or 'prop', if run locally choose dev
        :type env: string
        :param role: role assumed by this instance, choose between 'master' to run all tests, 'slave' to run a portion or debug or 'updateBoards'
        :type role: string
        :param configPath: relative path to config file formatted as required in the read me
        :type configPath: string
        :param testsPath: relative modular path to tests files
        :type testsPath: string
        :param extraParameters: [extraParamters useful to some of the roles, defaults to {}
        :param extraParameters: dict, optional
        """

        self.env = env
        self.role = role
        self.testsPath = testsPath
        self.params = extraParameters
        self.config = Config(configPath, self.env, self.role)
        self.logger = LogHandler.createLogHandler(self.env)
        self.testManager = TestManager(self.config, self.logger)
        if self.role in ['slave', 'updater'] or self.env == 'dev':
            self.publisherManager = PublisherManager(self.config, self.logger)
        if self.role == 'slave' or self.env == 'dev':
            self.fetcherManager = FetcherManager(self.config, self.logger)

    def executeResponsabilites(self):
        if self.role == 'master':
            self.dispatchWork(self.params['startIndex'])
        elif self.role == 'slave':
            self.runTests(self.params['filesNames'])
        elif self.role == 'updateBoards':
            self.updatePublishers()

    def dispatchWork(self, startIndex):

        if self.role != 'master':
            raise Exception(
                'The orchestrator has been instanciated with another role than master')
        tests = self.testManager.buildTests(self.testsPath)
        iterations = 0
        maxIterations = self.config.getValue('runConfiguration', 'iterations')
        while startIndex < len(tests) and iterations <= maxIterations:
            iterations += 1
            if iterations > maxIterations:
                # passes worload to next master
                self.logger.info(
                    'Reached max iterations for master run, passing to new master with start index of {}'.format(startIndex))
                self.callMaster(startIndex)
            else:
                # gets the next start Index
                testBatch, newstartIndex = self.testManager.subsetOfTests(
                    tests, startIndex, self.config.getValue('runConfiguration', 'batchSize'))
                filesNames = [test.name+'.yaml' for test in testBatch]
                self.logger.info(
                    'Calling slave with starting index of {0} and end index of {1}'.format(startIndex, newstartIndex))
                self.callSlave(filesNames)
                startIndex = newstartIndex

    def runTests(self, filesNames):
        # For running locally start index is passed in function call
        tests = self.testManager.buildTests(self.testsPath, filesNames)
        if len(tests) > 0:
            testsWithResults = self.fetcherManager.fetchResults(tests)
            self.testManager.computeResults(testsWithResults)
            self.publisherManager.publishResults(testsWithResults)

    def callMaster(self, startIndex):
        event = {'role': 'master', 'env': self.env, 'startIndex': startIndex}
        if self.env == 'prod':
            lambdaClient = LambdaClient(self.config, self.logger, self.env)
            lambdaClient.invokeFunction(
                'OverwatchMaster', 'async', dumps(event))
        else:
            self.dispatchWork(startIndex)

    def callSlave(self, filesNames):
        event = {'role': 'slave', 'env': self.env,
                 'filesNames': filesNames}
        if self.env == 'prod':
            lambdaClient = LambdaClient(self.config, self.logger, self.env)
            lambdaClient.invokeFunction(
                'OverwatchSlave', 'async', dumps(event))
            sleep(self.config.getValue(
                'runConfiguration', 'timeBetweenCalls'))
        else:
            self.runTests(filesNames)

    def updatePublishers(self):
        tests = self.testManager.buildTests(self.testsPath)
        self.logger.info('Updating publishers')
        self.publisherManager.updatePublishers(tests)

    def tearDown(self):
        if self.role == 'slave':
            self.fetcherManager.tearDown()
            self.publisherManager.tearDown()


def parseArg():
    args = CLIArgsParser().parseArgs()
    return args
