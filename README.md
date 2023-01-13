# MSFTProjects
Projects for CAROYS' MSFT focused projects

No internal information is planned to be published here.  If there is a concern, please reach out to the author.

## PowerKusto

PowerKusto is a powershell module designed to enable you to run queries against Kusto clusters directly from Powershell.

You can already do this with Log Analytics and Resource Graph, using the Azure cmdlets.  This now enables you to work with clusters in a similar fashion.

## ADF-DR

A script which was intended to help a user of Azure Data Factory take their factories, which have a long uncontrolled history of teams using them, and back it up to source control:

1. Pipelines (but only ones which had run recently)
2. Datasets
3. Linkedservices
4. The factory itself

The script accomplishes this by using the REST API to do a GET/PUT for backup/restore, and doing a little bit of fixup to the JSON during restore as needed.

## TraceHost

This script is intended as a diagnostic tool for constantly testing route/latency/reachability of a particular URI.  It will constantly run a traceroute to the host specified in the URI, and then do a ping to each node along the path, recording the time taken.  Then it will do a GET against the source URI and record the time taken there as well.

It outputs into a *.csv format which can be ingested into Log Analytics or similar log search solution, or you can import the *.csv's into Excel or PowerBI to play with the data.

Usage:  .\TraceHost.ps1 .\TraceHost.ps1 -pollinterval 120 -uri https://www.microsoft.com

This only works on Windows, due to the get-egressip function, which parses a webpage using a Win32 COM object to get your NAT'd public IP.  You could switch to a REST API method (if you can find a free one), and feasibly get this going on Powershell Core.

## AzureDasher

This uses template .json files from an Azure dashboard (microsoft.portal/dashboards), and creates a dashboard .json file with all available metrics, with multiple resources added to each graph.

This lets you look at the metrics of a pool of resources, one at a time, on one pane of glass to find outliers.  May not work with sub-resources (think: VMSS instances, Cosmos databases)

### Usage

$vms = @('/subscriptions/(subid)/resourceGroups/(RG)/providers/Microsoft.Compute/virtualMachines/(vmname1)',
'/subscriptions/(subid)/resourceGroups/(RG)/providers/Microsoft.Compute/virtualMachines/(vmname2)',
'/subscriptions/(subid)/resourceGroups/(RG)/providers/Microsoft.Compute/virtualMachines/(vmname3)')

.\AZVMDashboard.ps1 -ResourceList $vms -DashboardName "Cary's Dashboard" -OutputFile carydash.json -OpenNotepad $true 

Parameters:

**ResourceList** - should be an array of resourceID's

**DashboardName** - the displayname of the output dashboard file

**Outputfile** - The JSON file name (or full path) you want it to spit out

**OpenNotepad** - a lazy way of getting a look at the fruits of your labor.  Should be boolean value (default is $false).  Obviously only works on Windows.

## WebTester

A barebones multi-threaded .Net Core (C#) command line tool for generating HTTP requests.  Intended as a platform for tinkering with many variables to see what impact they have.

### Usage

`WebTester.exe -option optionvalue`

`-sleepms` - The time to sleep between making requests.  Default is 0.
`-minutes` - How many minutes to run the test for.  Default is 1.
`-uri` - What URI to hit.  Required.
`-threads` - How many threads to use.  Default is 1.
