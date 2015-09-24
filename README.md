bigquery
=====  
Written in go.  


## Install
```c
go get github.com/gotokatsuya/bigquery
```


## How to use it

```go
client, _ := bigquery.NewClient(email, pemKeyPath)
composer := bigquery.Composer{
	ProjectID: projectID,
	DatasetID: datasetID,
}
count, _ := composer.Count(client, tableID)
```