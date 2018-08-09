from ruamel.yaml import YAML
from glob import glob
import os
from time import time


class TestManager():
    """Container for group actions on tests

        :param config: Config instance
        :type config: Config
        :param logger: logger instance
        :type logger: logger
    """

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def findTestFiles(self, relativePath, filesNames=None):
        """Explores the relative path recursively to find matching files

        :param relativePath: relative path that files will be matched with
        :type relativePath: string
        :param filesNames: list of files names to restrict output, defaults to None
        :param filesNames: list of strings, optional
        :return: list of matched file names
        :rtype: list of strings
        """

        filePaths = glob(os.path.join(relativePath), recursive=True)
        filePaths.sort()
        if filesNames != None:
            filePaths = [fp for fp in filePaths if fp.split(
                '/')[-1] in filesNames]
        return filePaths

    def loadtestDictsFromFilePaths(self, testFilePaths):
        """Parses yaml files from given filepaths

        :param testFilePaths: file names to parse
        :type testFilePaths: list of strings
        :return: list of dict parsed from the yaml
        :rtype: list of dicts
        """

        testDicts = []
        yaml = YAML()
        for testFile in testFilePaths:
            with open(testFile) as f:
                testDict = yaml.load(f)
            testDicts.append(dict(testDict))
        return testDicts

    def testsFromYamlDict(self, yamlDict):
        """Build test from a yaml dict from a parsed file

        :param yamlDict: dict of parsed yaml file
        :type yamlDict: dict    
        :return: list of tests built from the yaml dict
        :rtype: list of tests
        """

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
        """Build tests from a list of test dicts obtained from parsing yaml files

        :param testDicts: list of dicts parsed from the yaml files
        :type testDicts: list of dicts
        :raises err: raises KeyError if parsed yaml files do not have required fields to build tests
        :return: list of tests
        :rtype: list of tests
        """

        tests = []
        for testDict in testDicts:
            try:
                tests = tests + self.testsFromYamlDict(testDict)
            except KeyError as err:
                raise err
        return tests

    def filterTests(self, tests, **criteria):
        """Filters a list of tests to match criteria

        :param tests: list of tests to filter
        :type tests: list of tests
        :return: list of filtered tests
        :rtype: list of tests
        """

        return [test for test in tests if test.isTest(**criteria)]

    def buildTests(self, relativePath, filesNames=None, onlyActive=True):
        """Finds matching files to relative path, parses them and build tests from those parsed dicts, optionnally filter with filesNames

        :param relativePath: path to find test files
        :type relativePath: string
        :param filesNames: only load those files, defaults to None
        :param filesNames: list of strings, optional
        :return: list of built tests
        :rtype: list
        """

        start = time()
        filePaths = self.findTestFiles(relativePath, filesNames)
        testDicts = self.loadtestDictsFromFilePaths(filePaths)
        tests = self.buildTestsFromDicts(testDicts)
        if onlyActive:
            tests = [test for test in tests if test.active == True]
        duration = time() - start
        self.logger.info(
            'Built {0} tests in {1:.2f} seconds'.format(len(tests), duration))
        return tests

    def computeResults(self, tests):
        """Computes test result for given list of tests by using each fetcher's result

        :param tests: list of tests for which to compute result
        :type tests: list of tests
        """

        for test in tests:
            test.computeResult()

    def subsetOfTests(self, tests, startIndex, maxsize):
        """Returns a subset of given test list, ideally os size maxsize but can be shorter
         in order to make sure tests with the same name are together

        :param tests: list of tests to extract subset from
        :type tests: list of tests
        :param startIndex: index to start from
        :type startIndex: int
        :param maxsize: maximum length of subset
        :type maxsize: int
        :return: subset of tests
        :rtype: list of tests
        """

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
        """Writes a batch of tests to file in the yaml format, grouping them by team and name

        :param tests: list of tests to write to file
        :type tests: list
        :param rootFolder: destination folder, defaults to './testsNewBuild/'
        :param rootFolder: str, optional
        """

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
    """Test object for quality test, ie one fetcher

        :param name: name of test
        :type name: string
        :param description: description of test
        :type description: string
        :param typ: quality or consistency
        :type typ: string
        :param team: team responsible for this test
        :type team: string
        :param active: whether the test is considered active
        :type active: bool
        :param fetchers: list of dictionnaries containing fetcher name and details that the test require
        :type fetchers: list
        :param publishers: list of dicts containing publisher name and details that the test uses
        :type publishers: list
        :param tags: tags that will be attached to published metrics
        :type tags: dict
        """

    def __init__(self, name, description, typ, team, active, fetchers, publishers, tags):
        self.name = name
        self.description = description
        self.type = typ
        self.team = team
        self.active = active
        self.fetchers = fetchers
        self.publishers = publishers
        self.tags = tags

    def isTest(self, **criteria):
        """Takes a number of criteria to check against the test attributes

        :return: true if the test checks out the criteria, false if different
        :rtype: bool
        """

        for name, value in criteria.items():
            if self.__dict__[name] != value:
                return False
        return True

    def computeResult(self):
        """Computes result for test
        """
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
    """Test object for consistency tests, ie two fetchers per test

        :param name: name of test
        :type name: string
        :param description: description of test
        :type description: string
        :param typ: quality or consistency
        :type typ: string
        :param team: team responsible for this test
        :type team: string
        :param active: whether the test is considered active
        :type active: bool
        :param fetchers: list of dictionnaries containing fetcher name and details that the test require
        :type fetchers: list
        :param publishers: list of dicts containing publisher name and details that the test uses
        :type publishers: list
        :param tags: tags that will be attached to published metrics
        :type tags: dict
        :param action: how to compute test result from fetchers result
        :type action: string
    """

    def __init__(self, name, description, typ, team, active, fetchers, publishers, tags, action):
        super().__init__(name, description, typ, team, active, fetchers, publishers, tags)
        self.action = action

    def computeResult(self):
        """Computes case result from the two queries result depending on the action defined in the case"""
        if self.action == 'difference':
            self.result = self.fetchers[0]['result'] - \
                self.fetchers[1]['result']
        elif self.action == 'division':
            self.result = self.fetchers[0]['result'] / \
                self.fetchers[1]['result']
