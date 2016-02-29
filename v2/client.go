package bigquery

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

type Client struct {
	BigQueryService *bigquery.Service
}

// NewClient ...
func NewClient(email, pemKeyPath string) (*Client, error) {
	pemKeyBytes, err := ioutil.ReadFile(pemKeyPath)
	if err != nil {
		return nil, err
	}
	conf := jwt.Config{
		Email:      email,
		PrivateKey: pemKeyBytes,
		Scopes:     []string{bigquery.BigqueryScope},
		TokenURL:   google.JWTTokenURL,
	}
	client := conf.Client(oauth2.NoContext)
	service, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}
	return &Client{BigQueryService: service}, nil
}

type Data struct {
	Headers []string
	Rows    [][]interface{}
	Err     error
}

func (c *Client) AsyncPagingQuery(projectID, datasetID, query string, opt Option, dataChan chan Data) {
	c.pagedQuery(projectID, datasetID, query, opt, dataChan)
}

func (c *Client) PagingQuery(projectID, datasetID, query string, opt Option) ([][]interface{}, []string, error) {
	return c.pagedQuery(projectID, datasetID, query, opt, nil)
}

func (c *Client) pagedQuery(projectID, datasetID, query string, opt Option, dataChan chan Data) ([][]interface{}, []string, error) {
	datasetRef := &bigquery.DatasetReference{
		ProjectId: projectID,
		DatasetId: datasetID,
	}

	req := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		Query:          query,
		Kind:           "json",
		MaxResults:     int64(opt.MaxResults),
		TimeoutMs:      int64(opt.TimeOutMs),
		UseQueryCache:  &opt.UseQueryCache,
	}

	qr, err := c.BigQueryService.Jobs.Query(projectID, req).Do()

	if err != nil {
		if dataChan != nil {
			dataChan <- Data{Err: err}
		}
		return nil, nil, err
	}

	var headers []string
	rows := [][]interface{}{}

	if qr.JobComplete {
		headers = c.makeHeaders(qr.Schema)
		rows = c.makeRows(qr.Schema, qr.Rows)
		if dataChan != nil {
			dataChan <- Data{Headers: headers, Rows: rows}
		}
	}

	if qr.TotalRows > uint64(opt.MaxResults) || !qr.JobComplete {
		resultChan := make(chan [][]interface{})
		headersChan := make(chan []string)

		go c.pageOverJob(len(rows), qr.JobReference, qr.PageToken, resultChan, headersChan)

	L:
		for {
			select {
			case h, ok := <-headersChan:
				if ok {
					headers = h
				}
			case newRows, ok := <-resultChan:
				if !ok {
					break L
				}
				if dataChan != nil {
					dataChan <- Data{Headers: headers, Rows: newRows}
				} else {
					rows = append(rows, newRows...)
				}
			}
		}
	}

	if dataChan != nil {
		close(dataChan)
	}

	return rows, headers, nil
}

func (c *Client) pageOverJob(rowCount int, jobRef *bigquery.JobReference, pageToken string, resultChan chan [][]interface{}, headersChan chan []string) error {

	qrc := c.BigQueryService.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId)
	if len(pageToken) > 0 {
		qrc.PageToken(pageToken)
	}

	qr, err := qrc.Do()
	if err != nil {
		close(resultChan)
		return err
	}

	if qr.JobComplete {
		headers := c.makeHeaders(qr.Schema)
		rows := c.makeRows(qr.Schema, qr.Rows)
		if headersChan != nil {
			headersChan <- headers
			close(headersChan)
		}

		resultChan <- rows
		rowCount = rowCount + len(rows)
	}

	if qr.TotalRows > uint64(rowCount) || !qr.JobComplete {
		if qr.JobReference == nil {
			c.pageOverJob(rowCount, jobRef, pageToken, resultChan, headersChan)
		} else {
			c.pageOverJob(rowCount, qr.JobReference, qr.PageToken, resultChan, nil)
		}
	} else {
		close(resultChan)
		return nil
	}
	return nil
}

func (c *Client) Query(projectID, datasetID, query string, opt Option) ([][]interface{}, error) {

	datasetRef := &bigquery.DatasetReference{
		ProjectId: projectID,
		DatasetId: datasetID,
	}

	req := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		Query:          query,
		Kind:           "json",
		MaxResults:     int64(opt.MaxResults),
		TimeoutMs:      int64(opt.TimeOutMs),
		UseQueryCache:  &opt.UseQueryCache,
	}

	results, err := c.BigQueryService.Jobs.Query(projectID, req).Do()
	if err != nil {
		return nil, err
	}

	numRows := int(results.TotalRows)
	if numRows > int(opt.MaxResults) {
		numRows = int(opt.MaxResults)
	}

	return c.makeRows(results.Schema, results.Rows), nil
}

func (c *Client) makeHeaders(bqSchema *bigquery.TableSchema) []string {
	headers := make([]string, len(bqSchema.Fields))
	for i, f := range bqSchema.Fields {
		headers[i] = f.Name
	}
	return headers
}

const (
	fieldTypeRecord = "RECORD"
)

func (c *Client) makeRows(bqSchema *bigquery.TableSchema, bqRows []*bigquery.TableRow) [][]interface{} {
	rows := make([][]interface{}, len(bqRows))
	for i, tableRow := range bqRows {
		row := make([]interface{}, len(bqSchema.Fields))
		for j, tableCell := range tableRow.F {
			schemaField := bqSchema.Fields[j]
			if schemaField.Type == fieldTypeRecord {
				row[j] = c.nestedFieldsData(schemaField.Fields, tableCell.V)
			} else {
				row[j] = tableCell.V
			}
		}
		rows[i] = row
	}
	return rows
}

func (c *Client) nestedFieldsData(nestedFields []*bigquery.TableFieldSchema, tableCellVal interface{}) interface{} {
	switch tcv := tableCellVal.(type) {
	// non-repeated RECORD
	case map[string]interface{}:
		data := make(map[string]interface{})
		vals, _ := tcv["f"]
		for i, f := range nestedFields {
			v := vals.([]interface{})[i]
			vv := v.(map[string]interface{})["v"]
			if f.Type == fieldTypeRecord {
				data[f.Name] = c.nestedFieldsData(f.Fields, vv)
			} else {
				data[f.Name] = vv
			}
		}
		return data
	// REPEATED RECORD
	case []interface{}:
		data := make([]map[string]interface{}, len(tcv))
		for j, mapv := range tcv {
			d := make(map[string]interface{})
			mapvv, _ := mapv.(map[string]interface{})["v"]
			vals, _ := mapvv.(map[string]interface{})["f"]
			for i, f := range nestedFields {
				v := vals.([]interface{})[i]
				vv := v.(map[string]interface{})["v"]
				if f.Type == fieldTypeRecord {
					d[f.Name] = c.nestedFieldsData(f.Fields, vv)
				} else {
					d[f.Name] = vv
				}
			}
			data[j] = d
		}
		return data
	default:
		return nil
	}
}

func (c *Client) Count(projectID, datasetID, tableID string, opt Option) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM [%s.%s];", datasetID, tableID)
	res, err := c.Query(projectID, datasetID, query, opt)
	if err != nil {
		return 0, err
	}
	if len(res) <= 0 {
		return 0, errors.New("Result of Query is empty.")
	}
	val, err := strconv.Atoi(res[0][0].(string))
	if err != nil {
		return 0, err
	}
	return val, nil
}
