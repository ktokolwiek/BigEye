import psycopg2
import time


class FetcherManager:
    """Container for different fetchers

    :param config: config instance holding credentials details for each fetcher
    :type config: Config
    :param logger: logger instance from logger module
    :type logger: logger
    """

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        fetchersToInit = config.getValue('runConfiguration', 'fetchers')
        self.fetchers = []
        for fetcher in fetchersToInit:
            if config.getValue('Fetchers', fetcher, 'type') == 'PostgresDB':
                self.fetchers.append(PostgresDB(
                    config.getValue('Fetchers', fetcher), logger, fetcher))

    def extractFetcher(self,  fetcherName):
        """Returns fetcher object with given name

        :param fetcherName: name of fetcher as defined in the config
        :type fetcherName: string
        :raises Exception: if fetcherName does not correspond to any of the fetchers
        :return: fetcher instance
        :rtype: fetcher
        """

        for fet in self.fetchers:
            if fet.fetcherName == fetcherName:
                return fet
        raise Exception('Could not find fetcher of name {} in the fetcher manager'.format(
            fetcherName))

    def fetchResults(self, tests):
        """Fetches result for each fetcher in each test

        :param tests: list of tests
        :type tests: list
        :return: list of tests with each fetcher dict (attribute of test) assigned a result, tests that failed are not returned
        :rtype: list
        """

        t1 = time.time()
        testsWithResults = []
        for test in tests:
            try:
                for fetcherDict in test.fetchers:
                    # measure run time for each test and send warning if one test is too long
                    maxTestDuration = int(self.config.getValue(
                        'runConfiguration', 'maxTestDuration'))
                    testStart = time.time()
                    fetcher = self.extractFetcher(fetcherDict['name'])
                    fetcherDict['result'] = fetcher.fetchResults(
                        fetcherDict['details'])
                    testDuration = time.time() - testStart
                    if testDuration > maxTestDuration:
                        self.logger.warning('test {0} with tags {1} has overran with {2:.2f} seconds runtime'.format(
                            test.name, test.tags, testDuration))

            except FetchError:
                pass
            else:
                testsWithResults.append(test)
        interval = time.time() - t1
        self.logger.info('Fetched values for {0} tests in {1:.2f} seconds'.format(
            len(testsWithResults), interval))
        return testsWithResults

    def tearDown(self):
        for fetcher in self.fetchers:
            fetcher.close()


class Fetcher:
    """Abstract class for interface like behaviour for fetchers"""

    def fetchResult(self, test):
        """Fetches result for each fetcher in the test

        :param test: test instance
        :type test: Test
        :raises NotImplementedError: if this method is not implemented in the given publisher class
        """

        print('The fetcher instance does not have the fetchResult method configured')
        raise NotImplementedError


class FetchError(Exception):
    """Exception raised if a fetcher errors out during fetching a result

    :param message: message to return when printing this error
    :type message: string
    """

    def __init__(self, message):

        self.message = message

    def __str__(self):
        return self.message


class PostgresDB(Fetcher):
    """Postgres sql client for fetching results in pg dbs

    :param dbconfig: connection credentials from config
    :type dbconfig: dict
    :param logger: logger instance
    :type logger: logger
    :param fetcherName: name to give to fetcher, used by FetcherManager
    :type fetcherName: string
    """

    def __init__(self, dbconfig, logger, fetcherName):

        self.logger = logger
        self.credentials = dbconfig
        self.openConnection()
        self.fetcherName = fetcherName

    def openConnection(self):
        """Opens connection to db, raise an exception if could not connect to db

        """

        try:
            self.conn = psycopg2.connect(host=self.credentials['host'], database=self.credentials['database'],
                                         port=5432, user=self.credentials['user'], password=self.credentials['password'])
            self.cur = self.conn.cursor()
        except:
            self.logger.error('could not connect to pg db')
            raise

    def fetchResults(self, details):
        """fetches results from pg db using info from details

        :param details: dictionnary that has a query key value pair
        :type details: dict
        :raises FetchError: if query returns zero rows
        :raises FetchError: if the query has an sql error
        :raises FetchError: if the db returns an internal error
        :return: value returned by query
        :rtype: int
        """

        self.cur.execute(details['query'])
        try:
            result = self.cur.fetchall()[0][0]
        except IndexError:
            self.logger.warn(
                'query for case returned zero rows')
            raise FetchError('Query returned zero rows')
        except psycopg2.ProgrammingError as err:
            self.logger.warn(
                'SQL error for case {0}'.format(err.args))
            # to exit transaction, otherwise connection stays in failed transaction and cannot accept further transactions
            self.conn.rollback()
            raise FetchError('SQL Error')
        except psycopg2.InternalError as err:
            self.logger.error("internal pg error: {}".format(err))
            raise FetchError('pg db error')
        return result

    def close(self):
        """Closes connection to db for clean exit"""
        self.cur.close()
        self.conn.close()
