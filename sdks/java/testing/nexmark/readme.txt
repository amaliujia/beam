# Run ZetaSQL on direct runner locally by only generate 10 events.
./gradlew :sdks:java:testing:nexmark:run \
    -Pnexmark.runner=":runners:direct-java" \
    -Pnexmark.args="
        --runner=DirectRunner
        --suite=SMOKE
        --streamTimeout=60
        --streaming=false
        --manageResources=false
        --monitorJobs=true
	--queryLanguage=sql --numEvents=10"

# Run ZetaSQL on spark runner locally
./gradlew :sdks:java:testing:nexmark:run \
    -Pnexmark.runner=":runners:spark" \
    -Pnexmark.args="
        --runner=SparkRunner
        --suite=SMOKE
        --streamTimeout=60
        --streaming=false
        --manageResources=false
        --monitorJobs=true
        --queryLanguage=sql"
