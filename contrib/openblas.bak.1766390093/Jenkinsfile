pipeline {
    agent { 
        docker {
            image 'osuosl/ubuntu-s390x'
        }
    }
    stages {
        stage('Build') {
            steps {
                sh 'make clean && make'
            }
        }
    }
}
