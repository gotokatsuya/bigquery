package bigquery

const (
	defaultMaxResults = 5000
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
