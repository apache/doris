## Doris Develop Environment based on docker
1、、Build docker image
```aidl
cd docker

docker build -t doris:v1.0  .

-- doris is docker image repository name and base is tag name , you can change them to what you like

```

2、Use docker image
```aidl
docker run -it --name doris  doris:v1.0
// if you want to build your local code ,you should execute:
docker run -it --name test -v **/incubator-doris:/var/local/incubator-doris  doris:v1.0

// then build source code
cd /var/local/incubator-doris

sh build.sh   -- build project

//execute unit test
sh run-ut.sh --run  -- run unit test

```
