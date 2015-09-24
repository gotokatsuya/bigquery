package bigquery

type Composer struct {
	ProjectID string
	DatasetID string
}

func (c *Composer) Query(client *Client, query string, max int) ([][]interface{}, error) {
	return client.Query(c.ProjectID, c.DatasetID, query, max)
}

func (c *Composer) PagingQuery(client *Client, query string, max int) ([][]interface{}, []string, error) {
	return client.PagingQuery(c.ProjectID, c.DatasetID, query, max)
}

func (c *Composer) AsyncPagingQuery(client *Client, query string, max int, f func([][]interface{}, []string)) {
	dataChan := make(chan Data)

	go client.AsyncPagingQuery(c.ProjectID, c.DatasetID, query, max, dataChan)

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
