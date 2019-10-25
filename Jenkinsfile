@Library('jenkins-joylib@v1.0.0') _

pipeline {

    agent {
        label joyCommonLabels(image_ver: '19.2.0')
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '30'))
        timestamps()
    }

    stages {
        stage('check') {
            steps{
                sh('make check')
            }
        }
        // Not yet ready for eng.git image builds.
        // stage('build image and upload') {
        //    steps {
        //        joyBuildImageAndUpload()
        //    }
        // }
    }

    post {
        always {
            joyMattermostNotification(channel: 'jenkins')
            joyMattermostNotification(channel: 'rebalancer')
        }
    }
}
