package main

import (
	"context"
	"strings"
	"time"

	"fmt"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strconv"
)

// productsDB is the subset of *pgxpool.Pool used by the catalog queries. It is
// an interface so the catalog can be unit-tested with a mock database.
type productsDB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type productCatalog struct {
	pb.UnimplementedProductCatalogServiceServer
	db productsDB
}

func (p *productCatalog) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (p *productCatalog) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "health check via Watch not implemented")
}

func (p *productCatalog) ListProducts(ctx context.Context, _ *pb.Empty) (*pb.ListProductsResponse, error) {
	time.Sleep(extraLatency)

	page := 1
	pageSize := 20
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("page"); len(v) > 0 {
			if parsed, err := strconv.Atoi(v[0]); err == nil && parsed > 0 {
				page = parsed
			}
		}
	}
	offset := (page - 1) * pageSize

	query := fmt.Sprintf(`
		SELECT p.id, p.name, p.description, p.picture_url, p.price_units, p.price_nanos, p.currency_code, 
		       COALESCE(string_agg(c.name, ','), '') as categories
		FROM products p
		LEFT JOIN product_categories pc ON p.id = pc.product_id
		LEFT JOIN categories c ON pc.category_id = c.id
		GROUP BY p.id, p.name, p.description, p.picture_url, p.price_units, p.price_nanos, p.currency_code
		ORDER BY p.id
		LIMIT %d OFFSET %d
	`, pageSize+1, offset)

	rows, err := p.db.Query(ctx, query)
	if err != nil {
		log.Errorf("failed to list products: %v", err)
		return nil, status.Errorf(codes.Internal, "database error")
	}
	defer rows.Close()

	var products []*pb.Product
	for rows.Next() {
		prod := &pb.Product{PriceUsd: &pb.Money{}}
		var catsStr string
		err := rows.Scan(&prod.Id, &prod.Name, &prod.Description, &prod.Picture,
			&prod.PriceUsd.Units, &prod.PriceUsd.Nanos, &prod.PriceUsd.CurrencyCode, &catsStr)
		if err != nil {
			log.Errorf("failed to scan product: %v", err)
			continue
		}
		if catsStr != "" {
			prod.Categories = strings.Split(catsStr, ",")
		}
		products = append(products, prod)
	}

	return &pb.ListProductsResponse{Products: products}, nil
}

func (p *productCatalog) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	time.Sleep(extraLatency)

	query := `
		SELECT p.id, p.name, p.description, p.picture_url, p.price_units, p.price_nanos, p.currency_code, 
		       COALESCE(string_agg(c.name, ','), '') as categories
		FROM products p
		LEFT JOIN product_categories pc ON p.id = pc.product_id
		LEFT JOIN categories c ON pc.category_id = c.id
		WHERE p.id = $1
		GROUP BY p.id, p.name, p.description, p.picture_url, p.price_units, p.price_nanos, p.currency_code
	`
	row := p.db.QueryRow(ctx, query, req.Id)
	prod := &pb.Product{PriceUsd: &pb.Money{}}
	var catsStr string
	err := row.Scan(&prod.Id, &prod.Name, &prod.Description, &prod.Picture,
		&prod.PriceUsd.Units, &prod.PriceUsd.Nanos, &prod.PriceUsd.CurrencyCode, &catsStr)
	if err != nil {
		log.Errorf("product not found or error: %v", err)
		return nil, status.Errorf(codes.NotFound, "no product with ID %s", req.Id)
	}
	if catsStr != "" {
		prod.Categories = strings.Split(catsStr, ",")
	}

	return prod, nil
}

func (p *productCatalog) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	time.Sleep(extraLatency)

	query := `
		SELECT p.id, p.name, p.description, p.picture_url, p.price_units, p.price_nanos, p.currency_code, 
		       COALESCE(string_agg(c.name, ','), '') as categories
		FROM products p
		LEFT JOIN product_categories pc ON p.id = pc.product_id
		LEFT JOIN categories c ON pc.category_id = c.id
		WHERE LOWER(p.name) LIKE $1 OR LOWER(p.description) LIKE $1
		GROUP BY p.id, p.name, p.description, p.picture_url, p.price_units, p.price_nanos, p.currency_code
	`
	searchParam := "%" + strings.ToLower(req.Query) + "%"
	rows, err := p.db.Query(ctx, query, searchParam)
	if err != nil {
		log.Errorf("failed to search products: %v", err)
		return nil, status.Errorf(codes.Internal, "database error")
	}
	defer rows.Close()

	var products []*pb.Product
	for rows.Next() {
		prod := &pb.Product{PriceUsd: &pb.Money{}}
		var catsStr string
		err := rows.Scan(&prod.Id, &prod.Name, &prod.Description, &prod.Picture,
			&prod.PriceUsd.Units, &prod.PriceUsd.Nanos, &prod.PriceUsd.CurrencyCode, &catsStr)
		if err != nil {
			log.Errorf("failed to scan product: %v", err)
			continue
		}
		if catsStr != "" {
			prod.Categories = strings.Split(catsStr, ",")
		}
		products = append(products, prod)
	}

	return &pb.SearchProductsResponse{Results: products}, nil
}
