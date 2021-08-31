package pkg

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/rs/zerolog"

	"golang.org/x/net/context"
)

type ArangoDB struct {
	db driver.Database
}

type VaccinationEdge struct {
	From       string `json:"_from"`
	To         string `json:"_to"`
	Collection string `json:"collection"`
}

func (graph *ArangoDB) createEdgeBetweenRunAndNodes(
	ctx context.Context,
	logger *zerolog.Logger,
	date string,
	ids []interface{},
) error {
	ctx = driver.WithQueryCount(ctx)

	if !strings.HasPrefix(date, "Runs/") {
		date = fmt.Sprintf("Runs/%s", date)
	}

	var edges []VaccinationEdge

	for _, id := range ids {
		edges = append(edges, VaccinationEdge{
			From:       date,
			To:         id.(string),
			Collection: "VaccinationsEdges",
		})
	}

	edgesCollection, err := graph.db.Collection(ctx, "VaccinationsEdges")
	if err != nil {
		logger.Err(err).Msg("An error occurred while trying to use VaccinationsEdges collection")
		return err
	}

	var raw []byte
	ctx = driver.WithRawResponse(ctx, &raw)
	stats, err := edgesCollection.ImportDocuments(ctx, edges, nil)

	if err != nil {
		logger.Err(err).Msg("An error occurred while trying to save edges")
		return err
	}

	logger.Info().Int("expected", len(edges)).Int64("actual", stats.Created).
		Int64("internal_errors", stats.Errors).Msg("Saved edges successfully")
	return nil
}

func (graph *ArangoDB) createEdgeBetweenOldAndNewNodes(
	ctx context.Context,
	logger *zerolog.Logger,
	prevIds []interface{},
	newIds []interface{},
) error {
	ctx = driver.WithQueryCount(ctx)

	var edges []VaccinationEdge

	for i, newId := range newIds {
		edges = append(edges, VaccinationEdge{
			From:       prevIds[i].(string),
			To:         newId.(string),
			Collection: "VaccinationsEdges",
		})
	}

	edgesCollection, err := graph.db.Collection(ctx, "VaccinationsEdges")
	if err != nil {
		logger.Err(err).Msg("An error occurred while trying to use VaccinationsEdges collection")
		return err
	}

	var raw []byte
	ctx = driver.WithRawResponse(ctx, &raw)
	stats, err := edgesCollection.ImportDocuments(ctx, edges, nil)

	if err != nil {
		logger.Err(err).Msg("An error occurred while trying to save edges")
		return err
	}

	logger.Info().Int("expected", len(edges)).Int64("actual", stats.Created).
		Int64("internal_errors", stats.Errors).Msg("Saved edges successfully")
	return nil
}

func (graph *ArangoDB) createNewVaccinationsDocuments(
	ctx context.Context,
	logger *zerolog.Logger,
	nodes []interface{},
) (metas driver.DocumentMetaSlice, err error) {
	assetsCol, err := graph.db.Collection(ctx, "Vaccinations")
	if err != nil {
		logger.Err(err).Msg("An error occurred while trying to use Vaccinations collection")
		return metas, fmt.Errorf("failed getting 'Assets' collection: %w", err)
	}

	metas, _, err = assetsCol.CreateDocuments(ctx, nodes)
	return metas, err
}

func ConnectToArango(endpoint, username, password, arangoCertificate, database string) (
	*ArangoDB,
	error,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	caCertificate, err := base64.StdEncoding.DecodeString(arangoCertificate)
	if err != nil {
		panic(err)
	}

	// Prepare TLS configuration
	tlsConfig := &tls.Config{}
	certpool := x509.NewCertPool()
	if success := certpool.AppendCertsFromPEM(caCertificate); !success {
		panic("Invalid certificate")
	}
	tlsConfig.RootCAs = certpool

	// Prepare HTTPS connection
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{endpoint},
		TLSConfig: tlsConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("failed creating HTTP connection: %w", err)
	}

	// Create client
	opts := driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(username, password),
	}
	c, err := driver.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("failed creating driver connection: %w", err)
	}

	db, err := c.Database(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed getting database %q: %w", database, err)
	}

	return &ArangoDB{db}, nil
}

func (graph *ArangoDB) CreateNewRunNode(
	ctx context.Context,
	date string,
) error {
	// Create new run document
	cursor, err := graph.db.Query(
		driver.WithQueryCount(ctx),
		"INSERT { '_key': @key , createdAt: @createdAt, collection: 'Runs' } INTO Runs",
		map[string]interface{}{
			"key":       fmt.Sprintf("%s", date),
			"createdAt": time.Now().Unix(),
		},
	)
	if err != nil {
		return err
	}

	return cursor.Close()
}

func (graph *ArangoDB) CreateEdgeBetweenRuns(
	ctx context.Context,
	previousDrift,
	currentDrift string,
) error {
	cursor, err := graph.db.Query(
		driver.WithQueryCount(ctx),
		"INSERT { _from: @from, _to: @to, collection: 'VaccinationsEdges' } INTO VaccinationsEdges",
		map[string]interface{}{
			"from": fmt.Sprintf("Runs/%s", previousDrift),
			"to":   fmt.Sprintf("Runs/%s", currentDrift),
		},
	)
	if err != nil {
		return err
	}

	return cursor.Close()
}

func (graph *ArangoDB) GetVaccinationNodes(
	ctx context.Context,
	date string,
) (vaccinationsNodes []map[string]interface{}, err error) {
	cursor, err := graph.db.Query(
		driver.WithQueryCount(ctx),
		"FOR v IN 1..1 ANY @runDate GRAPH 'runs-graph' FILTER v.collection == 'Vaccinations' RETURN v",
		map[string]interface{}{
			"runDate": fmt.Sprintf("Runs/%s", date),
		},
	)
	if err != nil {
		return vaccinationsNodes, fmt.Errorf("failed querying database: %w", err)
	}

	defer cursor.Close() // nolint: errcheck

	for {
		var node map[string]interface{}
		_, err := cursor.ReadDocument(ctx, &node)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return vaccinationsNodes, fmt.Errorf("failed reading document: %w", err)
		}

		vaccinationsNodes = append(vaccinationsNodes, node)
	}

	return vaccinationsNodes, nil
}

func (graph *ArangoDB) HandleNewCountries(
	ctx context.Context,
	logger *zerolog.Logger,
	date string,
	nodes []interface{},
) error {
	docs, err := graph.createNewVaccinationsDocuments(ctx, logger, nodes)
	if err != nil {
		logger.Error().Int("vaccinations", len(docs)).Str("error", err.Error()).
			Msg("An error occurred while trying to save in collection")
		return fmt.Errorf("failed creating vaccinations documents: %w", err)
	}

	var ids []interface{}

	for _, node := range docs {
		ids = append(ids, node.ID.String())
	}

	return graph.HandleNewEdges(ctx, logger, date, ids)
}

func (graph *ArangoDB) HandleChangedCountries(
	ctx context.Context,
	logger *zerolog.Logger,
	date string,
	changedNodes []interface{},
) error {
	var newPrevMap = make(map[string]interface{}, len(changedNodes))
	var nodes []interface{}
	for _, node := range changedNodes {
		nodeMap := node.(map[string]interface{})
		if prevId, ok := nodeMap["prevAssetId"]; ok {
			delete(nodeMap, "prevAssetId")
			nodes = append(nodes, nodeMap)
			newPrevMap[nodeMap["_key"].(string)] = prevId
		}
	}
	docs, err := graph.createNewVaccinationsDocuments(ctx, logger, nodes)
	if err != nil {
		logger.Err(err).Int("vaccinations", len(docs)).
			Msg("An error occurred while trying to save in collection")
		return err
	}

	var newIds []interface{}
	var prevIds []interface{}

	for _, node := range docs {
		prevId := newPrevMap[node.Key]
		newIds = append(newIds, node.ID.String())
		prevIds = append(prevIds, prevId)
	}
	err = graph.HandleNewEdges(
		ctx,
		logger,
		date,
		newIds)
	if err != nil {
		logger.Err(err).Int("count", len(newIds)).Msg("An error occurred while trying to save in collection")
		return err
	}
	err = graph.createEdgeBetweenOldAndNewNodes(
		ctx,
		logger,
		prevIds,
		newIds,
	)
	if err != nil {
		logger.Err(err).Interface("new_nodes", newIds).Interface("prev_nodes", prevIds).
			Msg("Failed creating edge between old and new nodes")
		return err
	}
	return nil
}

func (graph *ArangoDB) HandleNewEdges(
	ctx context.Context,
	logger *zerolog.Logger,
	date string,
	ids []interface{},
) error {
	err := graph.createEdgeBetweenRunAndNodes(
		ctx,
		logger,
		date,
		ids,
	)
	if err != nil {
		logger.Err(err).Int("documents", len(ids)).Msg("Zn error occurred while trying to save in edges")
		return err
	}
	return nil
}
