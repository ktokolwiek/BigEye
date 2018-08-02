import bigeye


# Path to the site-packages directory, for example ../dp-env/lib/python3.6/site-packages/
PATH_TO_VIRTUAL_ENV_PACKAGES = '../be_env/lib/python3.6/site-packages/'

# Packages to exclude such as linters
PACKAGES_TO_EXCLUDE = ['astroid', 'botocore', 'boto3', 'psycopg2', 'pip']

# Where the script will go to look for files to add to the zip package to add to the root of zip
LOOKUP_PATHS = ['../*.py', '../prod_config.yaml', '../tests/**/*.yaml']


def lambdaEntry(event, context):
    env = event['env']
    role = event['role']
    bg = bigeye.BigEye(env, role, event)
    bg.executeResponsabilites()


def updateLambda():
    zip = bigeye.Zipper(LOOKUP_PATHS, PATH_TO_VIRTUAL_ENV_PACKAGES,
                        packagesToExclude=PACKAGES_TO_EXCLUDE).buildZip()
    config = bigeye.Config('dev_config.yaml')
    logger = bigeye.LogHandler.createLogHandler('dev')
    cl = bigeye.LambdaClient(config, logger, 'dev')
    cl.updateFunction('OverwatchMaster', zip)
    cl.updateFunction('OverwatchSlave', zip)


if __name__ == '__main__':
    args = bigeye.parseArg().parseArgs()
    if args.mode == 'master':
        event = {"role": "master", "env": "dev", "startIndex": 0}
        lambdaEntry(event, None)
    elif args.mode == 'slave':
        event = {"role": "slave", "env": "dev", 'filesNames': [
            "./tests/data_insight/ccu_link_true_record_repo_customer.yaml"]}
        lambdaEntry(event, None)
    elif args.mode == 'updateBoards':
        event = {"role": "updater", "env": "dev"}
        lambdaEntry(event, None)
    elif args.mode == 'updateLambda':
        updateLambda()
