from time import sleep
from json import dumps
from .config import Config, LogHandler, CLIArgsParser
from .awsldaClient import LambdaClient, Zipper
from .tests import TestManager, QualityTest, ConsistencyTest
from .fetchers import FetcherManager
from .publishers import PublisherManager


class BigEye:
    """A big eye instance for good sight

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

    def __init__(self, env, role, configPath, testsPath, extraParameters={}):
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
        """Executes tasks based on the instance role"""

        if self.role == 'master':
            self.dispatchWork(self.params.get('startIndex', 0))
        elif self.role == 'slave':
            self.runTests(self.params['filesNames'])
        elif self.role == 'updateBoards':
            self.updatePublishers()

    def dispatchWork(self, startIndex):
        """Task execution for master instance, dispatches work to slaves

        :param startIndex: starting index, ie the number of tests already done by previous instance
        :type startIndex: int
        :raises Exception: if instance role is not master
        """

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
                    'Calling slave with files names {}'.format(set(filesNames)))
                self.callSlave(filesNames)
                startIndex = newstartIndex

    def runTests(self, filesNames):
        """Run tests for given filesNames, used by the slaves

        :param filesNames: name of files that need to be run
        :type filesNames: list of strings
        """

        # For running locally start index is passed in function call
        tests = self.testManager.buildTests(self.testsPath, filesNames)
        if len(tests) > 0:
            testsWithResults = self.fetcherManager.fetchResults(tests)
            self.testManager.computeResults(testsWithResults)
            self.publisherManager.publishResults(testsWithResults)

    def callMaster(self, startIndex):
        """For prod environment, calls a master lambda function to take over dispatching work, for local dispatches work

        :param startIndex: number of tests already dispatched for execution
        :type startIndex: int
        """

        event = {'role': 'master', 'env': self.env, 'startIndex': startIndex}
        if self.env == 'prod':
            lambdaClient = LambdaClient(self.config, self.logger, self.env)
            lambdaClient.invokeFunction(
                'OverwatchMaster', 'async', dumps(event))
        else:
            self.dispatchWork(startIndex)

    def callSlave(self, filesNames):
        """For prod environment, calls a slave lambda function wigh filenames as input, for dev executes those tests

        :param filesNames: file names of tests that need to be run
        :type filesNames: list of strings
        """

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
        """Update the publishers by using the test info
        """
        tests = self.testManager.buildTests(self.testsPath)
        self.logger.info('Updating publishers')
        self.publisherManager.updatePublishers(tests)

    def tearDown(self):
        """Closes the fetchers and publishers connections
        """

        if self.role == 'slave':
            self.fetcherManager.tearDown()
            self.publisherManager.tearDown()


def parseArg():
    args = CLIArgsParser().parseArgs()
    return args
