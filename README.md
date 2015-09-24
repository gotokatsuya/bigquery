bigquery
=====  
Written in go.  


## Install
```c
go get github.com/gotokatsuya/bigquery
```


## How to use it

```go

const (
	email      = "hoge@developer.gserviceaccount.com"
	pemKeyPath = "project-name.pem"

	projectID = "project-name"
	datasetID = "dataset-name"
	tableID   = "table-name"
)

func Count() (int, error) {
	client, err := bigquery.NewClient(email, pemKeyPath)
	if err != nil {
		return 0, err
	}
	if client == nil {
		return 0, errors.New("Client is nil.")
	}
	composer := bigquery.NewDefaultComposer(projectID, datasetID)
	count, err := composer.Count(client, tableID)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func GetOneRecord() ([][]interface{}, error) {
	client, err := bigquery.NewClient(email, pemKeyPath)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("Client is nil.")
	}
	composer := bigquery.NewDefaultComposer(projectID, datasetID)
	rows, err := composer.Query(client, fmt.Sprintf("SELECT * FROM [%s.%s] LIMIT 1;", datasetID, tableID))
	if err != nil {
		return nil, err
	}
	return rows, nil
}

```