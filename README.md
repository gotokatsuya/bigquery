bigquery
=====  
Written in go.  


## Install
```
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

// Count ...
func Count() (int, error) {
	client, err := bigquery.NewClient(email, pemKeyPath)
	if err != nil {
		return 0, err
	}
	composer := bigquery.NewDefaultComposer(projectID, datasetID)
	count, err := composer.Count(client, tableID)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// GetOneRecord ...
func GetOneRecord() ([][]interface{}, error) {
	client, err := bigquery.NewClient(email, pemKeyPath)
	if err != nil {
		return nil, err
	}
	composer := bigquery.NewDefaultComposer(projectID, datasetID)
	rows, err := composer.Query(client, fmt.Sprintf("SELECT * FROM [%s.%s] LIMIT 1;", datasetID, tableID))
	if err != nil {
		return nil, err
	}
	
	// rows[ number of row ][ number of column ]
	// So, you can get values are saved on BigQuery, `rows[0][0], rows[0][1], rows[0][2], ...`.
	// For example, description := rows[0][0].(string)
	
	return rows, nil
}

```
