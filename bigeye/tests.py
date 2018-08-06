from ruamel.yaml import YAML
from glob import glob
import os
from time import time


class TestManager():
    """Class to apply methods on a list of tests"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def findTestFiles(self, relativePath, filesNames=None):
        """Finds relative filepath to execution file of given parametered path"""
        filePaths = glob(os.path.join(relativePath), recursive=True)
        filePaths.sort()
        if filesNames != None:
            filePaths = [fp for fp in filePaths if fp.split(
                '/')[-1] in filesNames]
        return filePaths

    def loadtestDictsFromFilePaths(self, testFilePaths):
        """Parse given yaml files using ruamel modules and returns a list of dictionaries"""
        testDicts = []
        yaml = YAML()
        for testFile in testFilePaths:
            with open(testFile) as f:
                testDict = yaml.load(f)
            testDicts.append(dict(testDict))
        return testDicts

    def testsFromYamlDict(self, yamlDict):
        tests = []
        # metadata common for all tests in file
        name, description, testType, team = yamlDict['name'], yamlDict[
            'description'], yamlDict['type'], yamlDict['team']
        for metricname in yamlDict['metrics']:
            metricAttr = yamlDict['metrics'][metricname]
            active, tags = metricAttr['active'], dict(metricAttr['tags'])
            fetchers, publishers = [], []
            for fetcherName in metricAttr['fetchers']:
                # Commented maps object from ruamel.yaml are not great for copies, transforming to native dicts
                fetcherDetails = dict(metricAttr['fetchers'][fetcherName])
                fetchers.append(
                    {'name': fetcherName, 'details': fetcherDetails})
            for publisherName in metricAttr['publishers']:
                publisherDetails = dict(
                    metricAttr['publishers'][publisherName])
                publishers.append(
                    {'name': publisherName, 'details': publisherDetails})
            if testType == 'quality':
                tests.append(QualityTest(name, description, testType, team,
                                         active, fetchers, publishers, tags))
            elif testType == 'consistency':
                tests.append(ConsistencyTest(name, description, testType, team,
                                             active, fetchers, publishers, tags, yamlDict['action']))
        return tests

    def buildTestsFromDicts(self, testDicts):
        """Build list of tests from testDicts, handles missing arguments from yaml files"""
        tests = []
        for testDict in testDicts:
            try:
                tests = tests + self.testsFromYamlDict(testDict)
            except KeyError as err:
                raise err
        return tests

    def filterTests(self, tests, **criteria):
        """Filters a list of tests based on given criteria"""
        return [test for test in tests if test.isTest(**criteria)]

    def buildTests(self, relativePath, filesNames=None):
        """Finds, loads, builds and returns a filtered list of cases if the all arg is false"""
        start = time()
        filePaths = self.findTestFiles(relativePath, filesNames)
        testDicts = self.loadtestDictsFromFilePaths(filePaths)
        tests = self.buildTestsFromDicts(testDicts)
        duration = time() - start
        self.logger.info('Built tests in {0:.2f} seconds'.format(duration))
        return tests

    def computeResults(self, tests):
        for test in tests:
            test.computeResult()

    def subsetOfTests(self, tests, startIndex, maxsize):
        """Returns a subset of given test list, ideally os size maxsize but can be shorter
         in order to make sure tests with the same name are together"""
        # need a method to make sure tests with same names are published together for sync with datadog publisher
        # maxsize must be bigger than the max of number of test per test name
        if (startIndex+maxsize) >= len(tests) or tests[startIndex+maxsize-1].name != tests[startIndex+maxsize].name:
            subset = tests[startIndex:startIndex+maxsize]
        else:
            tests = tests[startIndex:startIndex+maxsize]
            # take away all tests that have the same name as the last one
            subset = [test for test in tests if not test.isTest(
                name=tests[-1].name)]
        return subset, startIndex+len(subset)

    def testToYAMLs(self, tests, rootFolder='./testsNewBuild/'):
        """Dumps one yaml file per test name in the given rootfolder"""
        # extract unique test names
        uniqueTestNames = set([c.name for c in tests])
        # group by test names to put them in same files
        for name in uniqueTestNames:
            yaml = YAML()
            yaml.default_flow_style = False
            testDict = None
            for t in tests:
                if t.name == name:
                    f = open(os.path.join(
                        rootFolder, t.team, name + '.yaml'), "w+")
                    if testDict == None:
                        testDict = t.toDict()
                    else:
                        key = 'metric' + str(len(testDict['metrics'])+1)
                        testDict['metrics'][key] = t.toDict()[
                            'metrics']['metric1']
            yaml.dump(testDict, f)


class QualityTest():

    def __init__(self, name, description, typ, team, active, fetchers, publishers, tags):
        """non self explanatory args: typ is the type of test(quality or consistency)"""
        self.name = name
        self.description = description
        self.type = typ
        self.team = team
        self.active = active
        self.fetchers = fetchers
        self.publishers = publishers
        self.tags = tags

    def isTest(self, **criteria):
        """Takes a number of filters and return true if the case matches those criteria"""
        for name, value in criteria.items():
            if self.__dict__[name] != value:
                return False
        return True

    def computeResult(self):
        self.result = self.fetchers[0]['result']

    def toDict(self):
        """Returns a dictionnary formatted similarly to the yaml test files"""
        d = {}
        d['name'], d['description'], d['type'], d['team'] = self.name, self.description, self.type, self.team
        d['metrics'] = {}
        d['metrics']['metric1'] = {}
        d['metrics']['metric1']['active'] = self.active
        d['metrics']['metric1']['fetchers'] = {
            f['name']: f['details'] for f in self.fetchers}
        d['metrics']['metric1']['publishers'] = {
            f['name']: f['details'] for f in self.publishers}
        d['metrics']['metric1']['tags'] = self.tags
        return d

    def __copy__(self):
        return type(self)(self.name, self.description, self.type, self.team, self.active, self.fetchers, self.publishers, self.tags)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


class ConsistencyTest(QualityTest):
    """This class is for all consistency tests that require extra attributes due to a higher number of queries"""

    def __init__(self, name, description, typ, team, active, fetchers, publishers, tags, action):
        """non self explanatory args: typ is the type of test(quality or consistency)"""
        super().__init__(name, description, typ, team, active, fetchers, publishers, tags)
        self.action = action

    def computeResult(self):
        """Computes case result from the two queries result depending on the action defined in the case"""
        if self.action == 'difference':
            self.result = self.fetchers[0]['result'] - \
                self.fetchers[0]['result']
        elif self.action == 'division':
            self.result = self.fetchers[0]['result'] / \
                self.fetchers[0]['result']
