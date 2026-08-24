#Requires -Version 5.0

<#
.SYNOPSIS
    Wexflow Server-Sent Events (SSE) Client script for PowerShell 5.0.

.DESCRIPTION
    Authenticates with the Wexflow REST API, starts a specified workflow job,
    subscribes to the corresponding SSE endpoint, and streams status updates until completion.

.PARAMETER BaseUrl
    The base API endpoint URL for the Wexflow instance (default: "http://localhost:8000/api/v1").

.PARAMETER Username
    The Wexflow username for authentication (default: "admin").

.PARAMETER Password
    The Wexflow password for authentication.

.PARAMETER WorkflowId
    The integer ID of the workflow to execute and monitor (default: 41).

.EXAMPLE
    .\Invoke-WexflowSseClient.ps1 -Username "admin" -Password "wexflow2018" -WorkflowId 41
#>

[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8000/api/v1",
    [string]$Username = "admin",
    [string]$Password = "wexflow2018",
    [int]$WorkflowId = 41
)

# Enforce TLS 1.2 for modern HTTP operations
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# Assembly load for HttpClient
Add-Type -AssemblyName System.Net.Http

# Functions
function Get-WexflowToken {
    param(
        [string]$Url,
        [string]$User,
        [string]$Pass
    )
    
    $loginUrl = "$Url/login"
    $body = @{
        username      = $User
        password      = $Pass
        stayConnected = $false
    } | ConvertTo-Json

    # Perform REST Login
    $response = Invoke-RestMethod -Uri $loginUrl -Method Post -Body $body -ContentType "application/json"
    
    if (-not $response.access_token) {
        throw "Failed to acquire JWT access token from response."
    }
    
    return $response.access_token
}

function Start-WexflowJob {
    param(
        [string]$Url,
        [string]$Token,
        [int]$WfId
    )
    
    $startUrl = "$Url/start?w=$WfId"
    $headers = @{
        Authorization = "Bearer $Token"
    }

    # Start the workflow via POST request
    $jobId = Invoke-RestMethod -Uri $startUrl -Method Post -Headers $headers
    return $jobId
}

function Listen-WexflowSse {
    <#
    .SYNOPSIS
        Connects to the Server-Sent Events (SSE) endpoint and reads streamed lines.
    .DESCRIPTION
        Uses System.Net.Http.HttpClient to establish an HTTP GET request with 
        HttpCompletionOption.ResponseHeadersRead. This allows line-by-line streaming of
        data payload lines prefixed with 'data: '.
    #>
    param(
        [string]$Url,
        [string]$Token
    )

    $handler = New-Object System.Net.Http.HttpClientHandler
    $client = New-Object System.Net.Http.HttpClient($handler)
    
    # Configure required HTTP Headers for SSE stream listening
    $request = New-Object System.Net.Http.HttpRequestMessage([System.Net.Http.HttpMethod]::Get, $Url)
    $request.Headers.Accept.Add((New-Object System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("text/event-stream")))
    $request.Headers.Authorization = New-Object System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", $Token)

    Write-Host "[SSE] Connecting to SSE stream..." -ForegroundColor Cyan

    try {
        # ResponseHeadersRead is crucial: it prevents HttpClient from buffering the whole stream into memory
        $responseTask = $client.SendAsync($request, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead)
        $response = $responseTask.Result

        if (-not $response.IsSuccessStatusCode) {
            throw "SSE Request failed with HTTP Status: $($response.StatusCode) - $($response.ReasonPhrase)"
        }

        $streamTask = $response.Content.ReadAsStreamAsync()
        $stream = $streamTask.Result
        $reader = New-Object System.IO.StreamReader($stream)

        Write-Host "[SSE] Connection established. Listening for events..." -ForegroundColor Green

        # Loop through stream line-by-line as data events arrive
        while (-not $reader.EndOfStream) {
            $lineTask = $reader.ReadLineAsync()
            $line = $lineTask.Result

            if (-not [string]::IsNullOrWhiteSpace($line) -and $line.StartsWith("data: ")) {
                # Extract JSON payload after 'data: ' prefix
                $jsonString = $line.Substring("data: ".Length)
                
                Write-Host "`n[SSE Event Received]" -ForegroundColor Yellow
                
                try {
                    $eventData = $jsonString | ConvertFrom-Json
                    
                    # Display structured output properties
                    Write-Host "  Workflow ID  : $($eventData.workflowId)"
                    Write-Host "  Job ID       : $($eventData.jobId)"
                    Write-Host "  Name         : $($eventData.name)"
                    Write-Host "  Status       : $($eventData.status)" -ForegroundColor Magenta
                    Write-Host "  Description  : $($eventData.description)"

                    # Server closes stream once status is terminal (e.g., Done, Failed, Stopped)
                    # Break loop after reading first terminal status frame
                    break
                }
                catch {
                    Write-Warning "Failed to parse raw SSE JSON payload: $_"
                    Write-Host "Raw Payload: $jsonString"
                }
            }
        }
    }
    finally {
        # Cleanup HTTP connections
        if ($null -ne $reader) { $reader.Dispose() }
        if ($null -ne $stream) { $stream.Dispose() }
        if ($null -ne $client) { $client.Dispose() }
        Write-Host "`n[SSE] Connection closed." -ForegroundColor Cyan
    }
}

# Main Execution Script Logic
try {
    Write-Host "1. Logging into Wexflow ($BaseUrl)..." -ForegroundColor White
    $jwtToken = Get-WexflowToken -Url $BaseUrl -User $Username -Pass $Password
    Write-Host "   Token retrieved successfully." -ForegroundColor Green

    Write-Host "2. Starting Workflow ID: $WorkflowId..." -ForegroundColor White
    $jobId = Start-WexflowJob -Url $BaseUrl -Token $jwtToken -WfId $WorkflowId
    Write-Host "   Job started successfully. Job ID: $jobId" -ForegroundColor Green

    # Construct SSE URL endpoint: /api/v1/sse/{workflowId}/{jobId}
    $sseEndpoint = "$BaseUrl/sse/$WorkflowId/$jobId"

    Write-Host "3. Subscribing to Wexflow SSE Endpoint..." -ForegroundColor White
    Listen-WexflowSse -Url $sseEndpoint -Token $jwtToken
}
catch {
    Write-Error "Execution Failed: $_"
}
