#Requires -Version 5.1

<#
.SYNOPSIS
    Wexflow Server-Sent Events (SSE) Client script for PowerShell 5.1.

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
    # stayConnected set to $true ensures the JWT token never expires (essential for multi-day jobs)
    $body = @{
        username      = $User
        password      = $Pass
        stayConnected = $true
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

function Watch-WexflowSse {
    <#
    .SYNOPSIS
        Connects to the Server-Sent Events (SSE) endpoint and reads streamed lines.
    .DESCRIPTION
        Uses System.Net.Http.HttpClient to establish an HTTP GET request with 
        HttpCompletionOption.ResponseHeadersRead. This allows line-by-line streaming of
        data payload lines prefixed with 'data: '.
    #>
    param(
        [string]$BaseUrl,
        [string]$Username,
        [string]$Password,
        [string]$SseUrl,
        [string]$InitialToken
    )

    # Define terminal workflow states that signal completion per Wexflow documentation
    $terminalStatuses = @("Done", "Failed", "Warning", "Stopped", "Rejected")
    $isTerminalStateReached = $false
    $currentToken = $InitialToken

    # Reconnection loop to handle network drops on multi-day running workflows
    while (-not $isTerminalStateReached) {
        $handler = New-Object System.Net.Http.HttpClientHandler
        $client = New-Object System.Net.Http.HttpClient($handler)
        
        # Prevent client-side timeout for multi-day operations
        $client.Timeout = [System.TimeSpan]::FromMilliseconds([System.Threading.Timeout]::Infinite)

        # Configure required HTTP Headers for SSE stream listening
        $request = New-Object System.Net.Http.HttpRequestMessage([System.Net.Http.HttpMethod]::Get, $SseUrl)
        $request.Headers.Accept.Add((New-Object System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("text/event-stream")))
        $request.Headers.Authorization = New-Object System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", $currentToken)

        Write-Host "[SSE] Connecting to SSE stream..." -ForegroundColor Cyan

        try {
            # ResponseHeadersRead is crucial: it prevents HttpClient from buffering the whole stream into memory
            $responseTask = $client.SendAsync($request, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead)
            $response = $responseTask.Result

            # Handle edge cases where server session resets or invalidates the token
            if ($response.StatusCode -eq [System.Net.HttpStatusCode]::Unauthorized) {
                Write-Warning "JWT token unauthorized. Re-authenticating with stayConnected=$true..."
                $currentToken = Get-WexflowToken -Url $BaseUrl -User $Username -Pass $Password
                continue
            }

            if (-not $response.IsSuccessStatusCode) {
                Write-Warning "SSE Request failed with HTTP Status: $($response.StatusCode) - $($response.ReasonPhrase). Retrying in 10 seconds..."
                Start-Sleep -Seconds 10
                continue
            }

            $streamTask = $response.Content.ReadAsStreamAsync()
            $stream = $streamTask.Result
            $reader = New-Object System.IO.StreamReader($stream)

            Write-Host "[SSE] Connection established. Listening for events..." -ForegroundColor Green

            # Loop through stream line-by-line as data events arrive
            while (-not $reader.EndOfStream) {
                # Protect against silent TCP deadlocks from intermediate proxies during idle days
                $cts = New-Object System.Threading.CancellationTokenSource([TimeSpan]::FromMinutes(5))

                try {
                    $lineTask = $reader.ReadLineAsync()
                    [System.Threading.Tasks.Task]::WaitAll(@($lineTask), $cts.Token)
                    $line = $lineTask.Result
                }
                catch {
                    Write-Warning "[SSE] Connection idle ping timeout (5 mins without frame). Re-establishing stream connection..."
                    break
                }
                finally {
                    $cts.Dispose()
                }

                if (-not [string]::IsNullOrWhiteSpace($line) -and $line.StartsWith("data: ")) {
                    # Extract JSON payload after 'data: ' prefix
                    $jsonString = $line.Substring("data: ".Length)
                    
                    Write-Host "`n[SSE Event Received: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')]" -ForegroundColor Yellow
                    
                    try {
                        $eventData = $jsonString | ConvertFrom-Json
                        
                        # Display structured output properties
                        Write-Host "  Workflow ID  : $($eventData.workflowId)"
                        Write-Host "  Job ID       : $($eventData.jobId)"
                        Write-Host "  Name         : $($eventData.name)"
                        Write-Host "  Status       : $($eventData.status)" -ForegroundColor Magenta
                        Write-Host "  Description  : $($eventData.description)"

                        # Break loop ONLY after reading a terminal status frame
                        if ($terminalStatuses -contains $eventData.status) {
                            $isTerminalStateReached = $true
                            break
                        }
                    }
                    catch {
                        Write-Warning "Failed to parse raw SSE JSON payload: $_"
                        Write-Host "Raw Payload: $jsonString"
                    }
                }
            }
        }
        catch {
            if (-not $isTerminalStateReached) {
                Write-Warning "SSE connection disconnected or timed out: $_. Reconnecting in 10 seconds..."
                Start-Sleep -Seconds 10
            }
        }
        finally {
            # Cleanup HTTP connections
            if ($null -ne $reader) { $reader.Dispose() }
            if ($null -ne $stream) { $stream.Dispose() }
            if ($null -ne $client) { $client.Dispose() }
            
            if ($isTerminalStateReached) {
                Write-Host "`n[SSE] Terminal status reached. Connection closed." -ForegroundColor Cyan
            }
        }
    }
}

# Main Execution Script Logic
try {
    Write-Host "1. Logging into Wexflow ($BaseUrl)..." -ForegroundColor White
    $jwtToken = Get-WexflowToken -Url $BaseUrl -User $Username -Pass $Password
    Write-Host "   Token retrieved successfully (stayConnected = true)." -ForegroundColor Green

    Write-Host "2. Starting Workflow ID: $WorkflowId..." -ForegroundColor White
    $jobId = Start-WexflowJob -Url $BaseUrl -Token $jwtToken -WfId $WorkflowId
    Write-Host "   Job started successfully. Job ID: $jobId" -ForegroundColor Green

    # Construct SSE URL endpoint: /api/v1/sse/{workflowId}/{jobId}
    $sseEndpoint = "$BaseUrl/sse/$WorkflowId/$jobId"

    Write-Host "3. Subscribing to Wexflow SSE Endpoint..." -ForegroundColor White
    Watch-WexflowSse -BaseUrl $BaseUrl -Username $Username -Password $Password -SseUrl $sseEndpoint -InitialToken $jwtToken
}
catch {
    Write-Error "Execution Failed: $_"
}
