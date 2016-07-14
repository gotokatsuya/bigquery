package bigquery

import (
	"errors"

	bigquery "google.golang.org/api/bigquery/v2"
)

const (
	defaultMaxResults = 5000
)

var (
	errOperationCreateTable = errors.New("error occured on bigquery.Insert")
	errOperationInsertAll   = errors.New("error occured on bigquery.InsertAll")
	errDataType             = errors.New("error data type")
)

type Composer struct {
	ProjectID  string
	DatasetID  string
	MaxResults int
}

func NewDefaultComposer(projectID, datasetID string) Composer {
	return Composer{
		ProjectID:  projectID,
		DatasetID:  datasetID,
		MaxResults: defaultMaxResults,
	}
}

func (c *Composer) Query(client *Client, query string) ([][]interface{}, error) {
	return client.Query(c.ProjectID, c.DatasetID, query, c.MaxResults)
}

func (c *Composer) PagingQuery(client *Client, query string) ([][]interface{}, []string, error) {
	return client.PagingQuery(c.ProjectID, c.DatasetID, query, c.MaxResults)
}

func (c *Composer) AsyncPagingQuery(client *Client, query string, f func([][]interface{}, []string)) {
	dataChan := make(chan Data)

	go client.AsyncPagingQuery(c.ProjectID, c.DatasetID, query, c.MaxResults, dataChan)

L:
	for {
		select {
		case d, ok := <-dataChan:
			if d.Err != nil {
				break L
			}

			if d.Rows != nil && d.Headers != nil {
				f(d.Rows, d.Headers)
			}

			if !ok {
				break L
			}
		}
	}
}

func (c *Composer) Count(client *Client, tableID string) (int, error) {
	return client.Count(c.ProjectID, c.DatasetID, tableID)
}

/*
 * Copyright (c) 2016 evalphobia
 * Released under the MIT license
 * https://github.com/YukinobuKurata/YouTubeMagicBuyButton/blob/master/MIT-LICENSE.txt
 *
 * Source: https://github.com/evalphobia/google-api-go-wrapper/blob/master/bigquery/schema.go
 */

// CreateTable creates the table with schema defined from given struct
func (c *Composer) CreateTable(client *Client, tableID string, schemaStruct interface{}) error {
	schema, err := convertToSchema(schemaStruct)
	if err != nil {
		return err
	}
	tbl := &bigquery.Table{
		Schema: schema,
		TableReference: &bigquery.TableReference{
			ProjectId: c.ProjectID,
			DatasetId: c.DatasetID,
			TableId:   tableID,
		},
	}
	_, err = client.BigQueryService.Tables.Insert(c.ProjectID, c.DatasetID, tbl).Do()
	if err != nil {
		return err
	}

	return nil
}

// InsertAll appends all of map data by using InsertAll api
func (c *Composer) InsertAll(client *Client, tableID string, data interface{}) error {
	rows, err := buildTableDataInsertAllRequest(data)
	if err != nil {
		return err
	}

	resp, err := client.BigQueryService.Tabledata.InsertAll(c.ProjectID, c.DatasetID, tableID, rows).Do()
	switch {
	case err != nil:
		return err
	case len(resp.InsertErrors) != 0:
		return errOperationInsertAll
	}

	return nil
}

func buildTableDataInsertAllRequest(data interface{}) (*bigquery.TableDataInsertAllRequest, error) {
	switch v := data.(type) {
	case []map[string]interface{}:
		return buildRowsFromMaps(v), nil
	case map[string]interface{}:
		return &bigquery.TableDataInsertAllRequest{
			Rows: []*bigquery.TableDataInsertAllRequestRows{buildRowFromMap(v)},
		}, nil
	}

	if list, ok := getSliceData(data); ok {
		rows := make([]*bigquery.TableDataInsertAllRequestRows, len(list))
		for i, v := range list {
			if !isStruct(v) {
				continue
			}

			row, err := buildRowsFromStruct(v)
			if err != nil {
				return nil, err
			}

			rows[i] = row
		}
		return &bigquery.TableDataInsertAllRequest{
			Rows: rows,
		}, nil
	}

	if isStruct(data) {
		row, err := buildRowsFromStruct(data)
		return &bigquery.TableDataInsertAllRequest{
			Rows: []*bigquery.TableDataInsertAllRequestRows{row},
		}, err
	}

	return nil, errDataType
}

func buildRowsFromMaps(list []map[string]interface{}) *bigquery.TableDataInsertAllRequest {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, len(list))
	for i, row := range list {
		rows[i] = buildRowFromMap(row)
	}

	return &bigquery.TableDataInsertAllRequest{
		Rows: rows,
	}
}

func buildRowFromMap(row map[string]interface{}) *bigquery.TableDataInsertAllRequestRows {
	return &bigquery.TableDataInsertAllRequestRows{
		Json: buildJSONValue(row),
	}
}

func buildJSONValue(row map[string]interface{}) map[string]bigquery.JsonValue {
	jsonValue := make(map[string]bigquery.JsonValue)
	for k, v := range row {
		jsonValue[k] = v
	}
	return jsonValue
}

func buildRowsFromStruct(data interface{}) (*bigquery.TableDataInsertAllRequestRows, error) {
	row, err := convertStructToMap(data)
	if err != nil {
		return nil, err
	}
	return buildRowFromMap(row), nil
}
