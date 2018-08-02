import psycopg2
import time


class FetcherManager:
    """Container for all fetchers"""

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
        for fet in self.fetchers:
            if fet.fetcherName == fetcherName:
                return fet
        raise Exception('Could not find fetcher of name {} in the fetcher manager'.format(
            fetcherName))

    def fetchResults(self, tests):
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
        print('The fetcher instance does not have the fetchResult method configured')
        raise NotImplementedError


class FetchError(Exception):
    """Exception raised if a fetcher errors out during fetching a result"""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class PostgresDB(Fetcher):
    """One instance per connection to a pg database"""

    def __init__(self, dbconfig, logger, fetcherName):
        """Gets credentials from passed config and opens a connection to db."""
        self.logger = logger
        self.credentials = dbconfig
        self.openConnection()
        self.fetcherName = fetcherName

    def openConnection(self):
        """Opens connection to the db with the credentials in the config passed during construction, catches all exceptions that could occur during connection set up"""
        try:
            self.conn = psycopg2.connect(host=self.credentials['host'], database=self.credentials['database'],
                                         port=5432, user=self.credentials['user'], password=self.credentials['password'])
            self.cur = self.conn.cursor()
        except:
            self.logger.error('could not connect to pg db')
            raise

    def fetchResults(self, details):
        """Execute query and returns the value of the first row and first column"""
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
