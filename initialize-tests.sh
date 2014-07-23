#/bina/bash

if [ -f ./initialize-env.sh ]; then
    . ./initialize-env.sh
fi

mvn3 clean install
mvn3 clean install -DskipTests -DskipITs -DperformRelease -f bigtop-test-framework/pom.xml
mvn3 clean install -DskipTests -DskipITs -DperformRelease -f bigtop-tests/test-artifacts/pom.xml
mvn3 clean install -DskipTests -DskipITs -DperformRelease -f bigtop-tests/test-execution/pom.xml
mvn3 clean install -DskipTests -DskipITs -DperformRelease -o -nsu -f bigtop-test-framework/pom.xml
mvn3 clean install -DskipTests -DskipITs -DperformRelease -o -nsu -f bigtop-tests/test-artifacts/pom.xml
mvn3 clean install -DskipTests -DskipITs -DperformRelease -o -nsu -f bigtop-tests/test-execution/pom.xml
