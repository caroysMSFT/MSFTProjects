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
