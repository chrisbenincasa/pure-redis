@Library("BuildUtils")
import com.curalate.jenkins.*

def utils = new BuildUtils()

utils.config.notificationEmails =  [ ] // emails to notify on build failure/prod deployments/etc

utils.build { BuildMetadata data ->
    def sbt = new Sbt(steps, data)

    String version = sbt.getAppVersion()

    setBuildName(version)

    stage('Build And Test ') {
        try {
            sbt.execute("clean +compile test doc")
        }
        finally {
            junit '**/target/test-reports/*.xml'
        }
    }

    if (data.isDeployable) {
        stage('Deploy') {
            sbt.execute("+publish")

            tagGitHub(version)
        }
    }
}
