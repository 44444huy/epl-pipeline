# Load .env
$envFile = "D:\EPL_PROJECT\epl-pipeline\.env"
$envVars = @{}
Get-Content $envFile | ForEach-Object {
    if ($_ -match "^([^#][^=]*)=(.*)$") {
        $envVars[$matches[1].Trim()] = $matches[2].Trim()
    }
}

$AWS_ACCESS_KEY_ID     = $envVars["AWS_ACCESS_KEY_ID"]
$AWS_SECRET_ACCESS_KEY = $envVars["AWS_SECRET_ACCESS_KEY"]
$AWS_REGION            = $envVars["AWS_REGION"]
$S3_BUCKET_RAW         = $envVars["S3_BUCKET_RAW"]

Write-Host "Region: $AWS_REGION"
Write-Host "Bucket: $S3_BUCKET_RAW"

$BASE_URL = "http://localhost:8083/connectors"

# ── Delete existing connectors ───────────────────────────────────
$existing = Invoke-RestMethod -Uri $BASE_URL -Method GET
foreach ($name in $existing) {
    Write-Host "Deleting existing connector: $name"
    Invoke-RestMethod -Uri "$BASE_URL/$name" -Method DELETE
}
Start-Sleep -Seconds 3

# ── Connector configs ────────────────────────────────────────────
$connectors = @(
    @{
        name   = "epl-matches-s3-sink"
        topics = "epl.matches"
        flush  = "10"
        rotate = "60000"
        path   = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH"
    },
    @{
        name   = "epl-events-s3-sink"
        topics = "epl.events"
        flush  = "10"
        rotate = "60000"
        path   = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH"
    },
    @{
        name   = "epl-standings-s3-sink"
        topics = "epl.standings"
        flush  = "5"
        rotate = "300000"
        path   = "'year'=YYYY/'month'=MM/'day'=dd"
    }
)

foreach ($c in $connectors) {
    $body = @{
        name   = $c.name
        config = @{
            "connector.class"                                  = "io.confluent.connect.s3.S3SinkConnector"
            "tasks.max"                                        = "1"
            "topics"                                           = $c.topics
            "s3.region"                                        = $AWS_REGION
            "s3.bucket.name"                                   = $S3_BUCKET_RAW
            "s3.part.size"                                     = "5242880"
            "flush.size"                                       = $c.flush
            "storage.class"                                    = "io.confluent.connect.s3.storage.S3Storage"
            "format.class"                                     = "io.confluent.connect.s3.format.json.JsonFormat"
            "topics.dir"                                       = "raw"
            "key.converter"                                    = "org.apache.kafka.connect.storage.StringConverter"
            "value.converter"                                  = "org.apache.kafka.connect.json.JsonConverter"
            "value.converter.schemas.enable"                   = "false"
            "partitioner.class"                                = "io.confluent.connect.storage.partitioner.TimeBasedPartitioner"
            "partition.duration.ms"                            = "3600000"
            "rotate.interval.ms"                               = $c.rotate
            "path.format"                                      = $c.path
            "timestamp.extractor"                              = "Wallclock"
            "locale"                                           = "en_US"
            "timezone"                                         = "UTC"
            "errors.tolerance"                                 = "all"
            "errors.deadletterqueue.topic.name"                = "epl.dlq"
            "errors.deadletterqueue.topic.replication.factor"  = "1"
        }
    } | ConvertTo-Json -Depth 5

    Write-Host "Deploying $($c.name)..."
    try {
        $response = Invoke-RestMethod `
            -Uri $BASE_URL `
            -Method POST `
            -ContentType "application/json" `
            -Body $body
        Write-Host "✅ Deployed: $($response.name)"
    } catch {
        Write-Host "❌ Failed: $_"
    }
}

# ── Check status ─────────────────────────────────────────────────
Start-Sleep -Seconds 5
Write-Host "`n=== Connector Status ==="
$list = Invoke-RestMethod -Uri $BASE_URL -Method GET
foreach ($name in $list) {
    $status = Invoke-RestMethod -Uri "$BASE_URL/$name/status" -Method GET
    $taskState = $status.tasks[0].state
    Write-Host "$name : $taskState"
}