from datadog import initialize, api
import time


class PublisherManager:
    """Container for all fetchers"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        publishersToInit = config.getValue('runConfiguration', 'publishers')
        self.publishers = []
        for publisher in publishersToInit:
            if config.getValue('Publishers', publisher, 'type') == 'Datadog':
                self.publishers.append(DatadogPublisher(
                    config.getValue('Publishers',  publisher), logger, publisher))

    def extractPublisher(self, publisherName):
        """Returns a publisher that has given type and name"""
        for pub in self.publishers:
            if pub.Name == publisherName:
                return pub
        raise Exception('Could not find publisher of name {} in the publisher manager'.format(
            publisherName))

    def getTestsForPublisher(self, publisher, tests):
        """
        Returns a list of test that have given publisher
        """
        return [test for test in tests if publisher.name in [p['name'] for p in test.publishers]]

    def publishResults(self, tests):
        for p in self.publishers:
            testsForThisPublisher = self.getTestsForPublisher(p, tests)
            try:
                p.publishResults(testsForThisPublisher)
            except PublishError:
                pass

    def updatePublishers(self, tests):
        for p in self.publishers:
            testsForThisPublisher = self.getTestsForPublisher(p, tests)
            self.logger.info(
                'Updating publisher {0} with {1} tests'.format(p.name, len(testsForThisPublisher)))
            p.update(testsForThisPublisher)

    def tearDown(self):
        for p in self.publishers:
            p.tearDown()


class Publisher:
    """Abstract class for interface like behaviour for publishers, not supposed to be instanciated"""

    def publishResults(self, tests):
        raise NotImplementedError(
            'The publisher instance does not implement the publishResults method')

    def update(self, tests):
        raise NotImplementedError(
            'The publisher instance does not implement the update method')

    def tearDown(self):
        raise NotImplementedError(
            'The publisher instance does not implement the tearDown method')


class PublishError(Exception):
    """Exception raised if a publisher errors out during fetching a result"""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class DatadogPublisher(Publisher):
    """Sends metric points to datadog by batch and updates timeboards"""

    def __init__(self, datadogConfig, logger, publisherName):
        """Initialise the datadog api with given datadogConfig, logger and publisherName from the config file"""
        initialize(api_key=datadogConfig['apiKey'],
                   app_key=datadogConfig['appKey'])
        self.batchSize = int(datadogConfig['batchSize'])
        self.config = datadogConfig
        self.logger = logger
        self.publisherType = 'Datadog'
        self.name = publisherName

    ##############################################
    ########## Metrics reporting #################
    ##############################################
    def buildMessageForDetailedGraphs(self, test):
        """Builds a datadog message from a case instance"""
        dashboardName = self.extractPublisherDetails(
            test)['dashboardName'].replace(' ', '_')
        metricName = "DataPolice." + dashboardName + "." + test.name
        msg = {'metric': metricName,
               'points': test.result,
               'tags': dict(test.tags)}
        return msg

    def buildMessageForSummaryGraphs(self, test):
        """Sends multiple values with same metric name but different tags for top level graphs"""
        dashboardName = self.extractPublisherDetails(
            test)['dashboardName'].replace(' ', '_')
        metricName = "DataPolice." + dashboardName
        ddTags = dict(test.tags)
        ddTags['test_name'] = test.name
        msg = {'metric': metricName,
               'points': test.result,
               'tags': ddTags}
        return msg

    def extractPublisherDetails(self, test):
        for p in test.publishers:
            if p['name'] == self.name:
                return p['details']

    def publishResults(self, tests):
        """Takes a list of tests to send to datadog by batch"""
        t1 = time.time()
        msgBuffer = []
        for i in range(len(tests)):
            msg1 = self.buildMessageForDetailedGraphs(tests[i])
            msg2 = self.buildMessageForSummaryGraphs(tests[i])
            msgBuffer.append(msg1)
            msgBuffer.append(msg2)
        resp = self.sendBatch(msgBuffer)
        if 'errors' in resp:
            self.logger.error(resp['errors'])
        t2 = time.time()
        self.logger.info('sent {0} metric points for  to datadog in {1:.2f} seconds'.format(
            len(tests), t2-t1))

    def sendBatch(self, msgBuffer):
        """Sends list of messages to datadog metric service"""
        return api.Metric.send(msgBuffer)

    def updateMetricsMetadata(self, tests):
        """Updates metric with metadata such as its description"""
        t1 = time.time()
        testsAlreadyUpdated = []
        for test in tests:
            if test.name not in testsAlreadyUpdated and test.description != '':
                metricName = "DataPolice." + test.team + "." + test.name
                params = {'description': test.description}
                print(api.Metadata.update(metric_name=metricName, **params))
                testsAlreadyUpdated.append(test.name)
        t2 = time.time()
        print('updated {0} descriptions in datadog in {1:.2f} seconds'.format(
            len(testsAlreadyUpdated), t2-t1))

    ##############################################
    ########### Boards utilities #################
    ##############################################

    def update(self, tests):
        # Need to group test by board type and board name
        timeboardsNames = set([pub['details']['dashboardName']
                               for test in tests for pub in test.publishers
                               if pub['name'] == self.name and pub['details']['typeOfDashboard'].lower() == 'timeboard'])
        screenboardNames = set([pub['details']['dashboardName']
                                for test in tests for pub in test.publishers
                                if pub['name'] == self.name and pub['details']['typeOfDashboard'].lower() == 'screenboard'])
        namesPerType = {'timeboard': timeboardsNames,
                        'screenboard': screenboardNames}

        for boardType in ('screenboard', 'timeboard'):
            for boardName in namesPerType[boardType]:
                testsPerDashboard = []
                for test in tests:
                    details = self.extractPublisherDetails(test)
                    if details['typeOfDashboard'].lower() == boardType and details['dashboardName'] == boardName:
                        testsPerDashboard.append(test)
                self.logger.info('Updating {0} {1} with {2} tests'.format(
                    boardType, boardName, len(testsPerDashboard)))
                if boardType == 'timeboard':
                    self.updateTimeBoard(boardName, testsPerDashboard)
                elif boardType == 'screenboard':
                    self.updateScreenboard(boardName, testsPerDashboard)

    ##############################################
    ########## Timeboard utilities ###############
    ##############################################

    def generateDetailedGraph(self, case, TBName):
        """Generates timeseries line graphs for given case with a breakdown per desco"""
        graph = {
            "title": case.name,

            "definition": {
                "viz": "timeseries",
                "requests": [
                    {
                        "q": "avg:DataPolice." + TBName.replace(' ', '_') + "." + case.name + "{*} by {desco}"
                    }
                ],
            }
        }
        return graph

    def generateTopList(self, TBName):
        """Returns a dictionary of a summary graph listing top offenders by descending order"""
        graph = {
            "title": "top offenders ",

            "definition": {
                "requests": [
                    {
                        "q": "top(avg:DataPolice." + TBName.replace(' ', '_') + "{*} by {test_name,desco}, 50, 'last', 'desc')",
                    }
                ],
                "viz": "toplist",
            }
        }
        return graph

    def generateTopChange(self, TBName):
        """Returns a dictionary of a change graph"""
        graph = {
            "title": "change vs previous day ",

            "definition": {
                "requests": [
                    {
                        "q": "avg:DataPolice." + TBName.replace(' ', '_') + "{*} by {test_name,desco}",
                        "compare_to": "day_before",
                        "change_type": "absolute",
                        "order_by": "change",
                        "order_dir": "desc",
                        "extra_col": "present",
                        "increase_good": False
                    }
                ],
                "viz": "change"
            }
        }
        return graph

    def generateDahsboardGraphs(self, tests, TBName):
        """Generates the list of graphs for the timeboards"""
        graphs = [self.generateTopList(TBName), self.generateTopChange(TBName)]
        testsAlreadyLoaded = []
        for test in tests:
            if test.name not in testsAlreadyLoaded:
                graph = self.generateDetailedGraph(test, TBName)
                graphs.append(graph)
                testsAlreadyLoaded.append(test.name)
        return graphs

    def createTimeBoard(self, TBName, tests):
        """Takes a title, description and a list of cases and creates a timeboard of timeseries graphs for the cases"""
        graphs = self.generateDahsboardGraphs(tests, TBName)
        resp = api.Timeboard.create(
            title=TBName, description='', graphs=graphs)
        if 'errors' in resp:
            self.logger.error(resp)

    def apiUpdateTB(self, boardID, title,  graphs):
        """Takes the board ID, title, description and graphs and updates corresponding board"""
        resp = api.Timeboard.update(
            boardID,
            title=title,
            description='',
            graphs=graphs)
        if 'errors' in resp:
            self.logger.error('Could not update timeboard {}'.format(resp))

    def updateTimeBoard(self, TBName, tests):
        """Updates the timeboard for the given team, typ with graphs built from the cases"""
        if len(tests) > 0:
            graphs = self.generateDahsboardGraphs(tests, TBName)
            try:
                id = self.getIdOfTimeboard(TBName)
            except Exception as e:
                if e.__str__() == 'Timeboard does not exist':
                    self.createTimeBoard(TBName, tests)
                    self.logger.info(
                        'Created timeboard {}'.format(TBName))
            else:
                self.apiUpdateTB(id, TBName,  graphs)
                self.logger.info(
                    'Updated timeboard {0} '.format(TBName))

    def getAllTimeBoards(self):
        """To get the ids of the board in order to be able to update them, as name is not enough"""
        res = api.Timeboard.get_all()
        return res

    def getIdOfTimeboard(self, name):
        """Get all tbs and loops over them till it finds the id required"""
        allTBs = self.getAllTimeBoards()['dashes']
        for tb in allTBs:
            if tb['title'] == name:
                return tb['id']
        e = Exception('Timeboard does not exist')
        raise e

    ##################################################
    ########### Screenboard Utilities ################
    ##################################################

    def imagesForSB(self):
        imgs = [{
            "type": "image",

            "height": 2,
            "width": 2,
            "x": i*5,
            "y": 0,

            "url": "http://www.bboxx.co.uk/wp-content/themes/bboxx/stylesheets/images/bboxx-logo.png"
        } for i in range(23)]
        return imgs

    def generateTopWidget(self, SBName):
        """Returns a dictionary of a summary widget listing top offenders by descending order"""
        widget = {
            "type": "toplist",
            "title": True,
            "title_size": 16,
            "title_align": "left",
            "title_text": "Top offenders",

            "height": 20,
            "width": 50,

            "y": 3,
            "x": 1,

            "time": {
                # Choose from: [1m, 5m, 10m, 15m, 30m, 1h, 4h, 1d, 2d, 1w, 1mo, 3mo, 6mo, 1y]
                "live_span": "1d"
            },

            "tile_def": {
                "viz": "toplist",
                "requests": [
                    {
                        "q": "top(avg:DataPolice." + SBName.replace(' ', '_') + "{*} by {test_name,desco}, 50, 'last', 'desc')"
                    }
                ],
                "events": [
                    {}
                ]
            }
        }
        return widget

    def generateChangeWidget(self, SBName):
        """Returns a dictionary of a change graph"""
        widget = {
            "type": "change",
            "title": True,
            "title_size": 16,
            "title_align": "left",
            "title_text": "Change",

            "height": 20,
            "width": 52,

            "y": 3,
            "x": 55,

            "time": {
                # Choose from: [1m, 5m, 10m, 15m, 30m, 1h, 4h, 1d, 2d, 1w, 1mo, 3mo, 6mo, 1y]
                "live_span": "1d"
            },

            "tile_def": {
                "viz": "change",
                "requests": [
                    {
                        "q": "avg:DataPolice." + SBName.replace(' ', '_') + "{*} by {test_name,desco}",
                        "compare_to": "day_before",
                        "change_type": "absolute",
                        "order_by": "change",
                        "order_dir": "desc",
                        "extra_col": "present",
                        "increase_good": False
                    }
                ],
                "events": [
                    {}
                ]
            }
        }
        return widget

    def generateTimeseriesForSB(self, test, SBName, x, y):
        """Generates timeseries line graphs for given test with a breakdown per desco"""
        timeSeries = {
            "type": "timeseries",

            "title": True,
            "title_size": 16,
            "title_align": "left",
            "title_text": test.name,

            "height": 13,
            "width": 35,

            "y": y,
            "x": x,

            "tile_def": {
                "viz": "timeseries",
                "requests": [
                    {
                        "q": "avg:DataPolice." + SBName.replace(' ', '_') + "." + test.name + "{*} by {desco}"
                    }
                ],
                "events": [
                    {}
                ]
            }
        }
        return timeSeries

    def generateWidgetsForSB(self, tests, SBName):
        widgets = self.imagesForSB()
        widgets.append(self.generateTopWidget(SBName))
        widgets.append(self.generateChangeWidget(SBName))
        yStart, graphsPerRow, i = 30, 3, 0
        testsWithWidget = []
        for test in tests:
            if test.name not in testsWithWidget:
                x = 1 + 37*(i % graphsPerRow)
                y = yStart + 17*(i//graphsPerRow)
                ts = self.generateTimeseriesForSB(test, SBName, x, y)
                widgets.append(ts)
                testsWithWidget.append(test.name)
                i += 1
        return widgets

    def generateTemplateVariablesForSB(self):
        template_variables = [{
            "name": "var",
            "prefix": "desco",
            "default": "desco:*"
        }]
        return template_variables

    def createScreenboard(self, SBName, tests):
        """Takes a title, description and a list of tests and creates a screenboard of timeseries graphs for the tests"""
        widgets = self.generateWidgetsForSB(tests, SBName)
        tv = self.generateTemplateVariablesForSB()
        resp = api.Screenboard.create(
            board_title=SBName, description='', widgets=widgets, width=1024)
        if 'errors' in resp:
            self.logger.error(resp)

    def apiUpdateSB(self, boardID, title,  widgets, tv):
        """Takes the board ID, title, description and graphs and updates corresponding board"""
        resp = api.Screenboard.update(
            boardID,
            board_title=title,
            description='',
            widgets=widgets,
            width=1024)
        if 'errors' in resp:
            self.logger.error('Could not update screenboard {}'.format(resp))

    def updateScreenboard(self, SBname, tests):
        """Updates the screenboard of given name graphs built from the cases"""
        if len(tests) > 0:
            widgets = self.generateWidgetsForSB(tests, SBname)
            tv = self.generateTemplateVariablesForSB()
            try:
                id = self.getIdOfScreenboard(SBname)
            except Exception as e:
                if e.__str__() == 'Screenboard does not exist':
                    self.createScreenboard(SBname, tests)
                    self.logger.info(
                        'Created screenboard {0}'.format(SBname))
            else:
                self.apiUpdateSB(id, SBname, widgets, tv)
                self.logger.info(
                    'Updated screenboard {0}'.format(SBname))

    def getAllScreenboards(self):
        """To get the ids of the board in order to be able to update them, as name is not enough"""
        res = api.Screenboard.get_all()
        return res

    def getIdOfScreenboard(self, name):
        """Get all screenboards and returns id if a a name matches the parameter, otherwise raises an exception"""
        allSBs = self.getAllScreenboards()['screenboards']
        for sb in allSBs:
            if sb['title'] == name:
                return sb['id']
        e = Exception('Screenboard does not exist')
        raise e

    def tearDown(self):
        pass
