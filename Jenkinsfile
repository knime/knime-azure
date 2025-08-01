#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2025-12'

library "knime-pipeline@$BN"

properties([
    // provide a list of upstream jobs which should trigger a rebuild of this job
    pipelineTriggers([
        // knime-tp -> knime-base -> knime-svg -> knime-js-core -> knime-workbench
        upstream('knime-workbench/' + env.BRANCH_NAME.replaceAll('/', '%2F'))
    ]),
    parameters(workflowTests.getConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])


try {
    knimetools.defaultTychoBuild('org.knime.update.azure')

    workflowTests.runTests(
        dependencies: [
                // yes, we really need all this stuff. knime-cloud pulls in most of it...
    		repositories:  [
    		    'knime-aws',
    		    'knime-azure',
    		    'knime-base-expressions',
    		    'knime-bigdata',
    		    'knime-bigdata-externals',
    		    'knime-cloud',
    		    'knime-credentials-base',
    		    'knime-database',
    		    'knime-database-proprietary',
    		    'knime-expressions',
    		    'knime-filehandling',
    		    'knime-gateway',
    		    'knime-js-base',
    		    'knime-kerberos',
    		    'knime-office365',
    		    'knime-rest',
    		    'knime-scripting-editor',
    		    'knime-streaming',
    		    'knime-textprocessing',
    		    'knime-xml'
		    ],
            ius: [
                'org.knime.features.database.extensions.sqlserver.driver.feature.group'
            ]
        ]
    )

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}
/* vim: set shiftwidth=4 expandtab smarttab: */
