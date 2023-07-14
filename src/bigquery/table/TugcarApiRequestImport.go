/*
	=================================

BigQuery
Dataset::gcs_logging
Table::t_flight_record
テーブルの構造をここで指定する
* =================================
*/
package table

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	DATASET_TUGCAR_API_REQUEST_LOGGING = "gcs_logging"                         //dataset
	TABLE_TUGCAR_API_REQUEST_LOGGING   = "t_tugcar_api_request_logging_import" //tableId
)

// データ挿入用テーブル定義
type TugcarApiRequestImport struct {
	Cdt           time.Time `bigquery:"cdt"`
	UrlPath       string    `bigquery:"url_path"`
	Type          string    `bigquery:"type"`
	ClientId      string    `bigquery:"client_id"`
	CurrentParams string    `bigquery:"current_params"`
	CurrentUrl    string    `bigquery:"current_url"`
	QueryTags     string    `bigquery:"query_tags"`
	QueryItemId   string    `bigquery:"query_item_id"`
	CustomerUuid  string    `bigquery:"customer_uuid"`
	WhatYaId      string    `bigquery:"hmt_id"`
	SearchFrom    string    `bigquery:"search_from"`
	Timestamp     time.Time `bigquery:"timestamp"`
}
