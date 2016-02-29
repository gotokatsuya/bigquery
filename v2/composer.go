package bigquery

type Composer struct {
	ProjectID string
	DatasetID string
	Option    Option
}

type Option struct {
	MaxResults    int
	TimeOutMs     int
	UseQueryCache bool
}

const (
	defaultMaxResults = 5000
	// 120s ( bigquery's default is 10s )
	defaultTimeOutMs     = 120000
	defaultUseQueryCache = true
)

func NewDefaultOption() Option {
	return Option{
		MaxResults:    defaultMaxResults,
		TimeOutMs:     defaultTimeOutMs,
		UseQueryCache: defaultUseQueryCache,
	}
}

func NewDefaultComposer(projectID, datasetID string) Composer {
	return Composer{
		ProjectID: projectID,
		DatasetID: datasetID,
		Option:    NewDefaultOption(),
	}
}

func (c *Composer) Query(client *Client, query string) ([][]interface{}, error) {
	return client.Query(c.ProjectID, c.DatasetID, query, c.Option)
}

func (c *Composer) PagingQuery(client *Client, query string) ([][]interface{}, []string, error) {
	return client.PagingQuery(c.ProjectID, c.DatasetID, query, c.Option)
}

func (c *Composer) AsyncPagingQuery(client *Client, query string, f func([][]interface{}, []string)) {
	dataChan := make(chan Data)

	go client.AsyncPagingQuery(c.ProjectID, c.DatasetID, query, c.Option, dataChan)

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
	opt := NewDefaultOption()
	opt.MaxResults = 1
	return client.Count(c.ProjectID, c.DatasetID, tableID, opt)
}
