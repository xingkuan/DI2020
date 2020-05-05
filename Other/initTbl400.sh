export VSYCNHOME=/home/jenadmin/vSync400/Java
export CLASSPATH=$CLASSPATH:$VSYCNHOME/libs/log4j-api-2.7.jar:$VSYCNHOME/libs/log4j-core-2.7.jar:$VSYCNHOME/libs/jt400.jar:$VSYCNHOME/libs/vertica-jdbc-8.1.1-0.jar
cd $VSYCNHOME
java -Djava.security.egd=file:/dev/../dev/urandom -cp ./bin:$CLASSPATH com.guess.vsync400.OVSinit $1
