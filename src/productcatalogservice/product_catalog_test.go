// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"testing"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func productColumns() []string {
	return []string{"id", "name", "description", "picture_url", "price_units", "price_nanos", "currency_code", "categories"}
}

func TestGetProductExists(t *testing.T) {
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	pool.ExpectQuery(`SELECT p.id, p.name`).
		WithArgs("abc003").
		WillReturnRows(pgxmock.NewRows(productColumns()).
			AddRow("abc003", "Product Alpha Two", "desc", "pic", int64(1), int32(500000000), "USD", "clothing,accessories"))

	catalog := &productCatalog{db: pool}
	product, err := catalog.GetProduct(context.Background(), &pb.GetProductRequest{Id: "abc003"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := product.Name, "Product Alpha Two"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := product.PriceUsd.CurrencyCode, "USD"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestGetProductNotFound(t *testing.T) {
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	pool.ExpectQuery(`SELECT p.id, p.name`).
		WithArgs("abc005").
		WillReturnError(pgx.ErrNoRows)

	catalog := &productCatalog{db: pool}
	_, err = catalog.GetProduct(context.Background(), &pb.GetProductRequest{Id: "abc005"})
	if got, want := status.Code(err), codes.NotFound; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestListProducts(t *testing.T) {
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	pool.ExpectQuery(`SELECT p.id, p.name`).
		WillReturnRows(pgxmock.NewRows(productColumns()).
			AddRow("abc001", "Product Alpha One", "d", "p", int64(0), int32(0), "USD", "clothing").
			AddRow("abc002", "Product Delta", "d", "p", int64(0), int32(0), "USD", "clothing").
			AddRow("abc003", "Product Alpha Two", "d", "p", int64(0), int32(0), "USD", "clothing").
			AddRow("abc004", "Product Gamma", "d", "p", int64(0), int32(0), "USD", "clothing"))

	catalog := &productCatalog{db: pool}
	products, err := catalog.ListProducts(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(products.Products), 4; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestSearchProducts(t *testing.T) {
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	pool.ExpectQuery(`SELECT p.id, p.name`).
		WithArgs("%alpha%").
		WillReturnRows(pgxmock.NewRows(productColumns()).
			AddRow("abc001", "Product Alpha One", "d", "p", int64(0), int32(0), "USD", "clothing").
			AddRow("abc003", "Product Alpha Two", "d", "p", int64(0), int32(0), "USD", "clothing"))

	catalog := &productCatalog{db: pool}
	resp, err := catalog.SearchProducts(context.Background(),
		&pb.SearchProductsRequest{Query: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(resp.Results), 2; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}
