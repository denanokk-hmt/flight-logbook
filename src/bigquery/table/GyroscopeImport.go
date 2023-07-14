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
	DATASET_GYROSCOPE = "gcs_logging"        //dataset
	TABLE_GYROSCOPE   = "t_gyroscope_import" //tableId
)

// データ挿入用テーブル定義
type GyroscopeImport struct {
	BatchId      string    `bigquery:"batch_id"`
	Cdt          time.Time `bigquery:"cdt"`
	InsertId     string    `bigquery:"insert_id"`
	Host         string    `bigquery:"host"`
	Url          string    `bigquery:"url"`
	Path         string    `bigquery:"path"`
	Ip           string    `bigquery:"ip"`
	UserAgent    string    `bigquery:"user_agent"`
	Referrer     string    `bigquery:"referrer"`
	ClientId     string    `bigquery:"client_id"`
	CustomerUuid string    `bigquery:"customer_uuid"`
	CustomerId   string    `bigquery:"customer_id"`
	WhatyaId     string    `bigquery:"hmt_id"`
	CategoryId   string    `bigquery:"category_id"`
	Type         string    `bigquery:"type"`
	Id           string    `bigquery:"id"`
	Label        string    `bigquery:"label"`
	Value        string    `bigquery:"value"`
	WyEvent      string    `bigquery:"wy_event"`
	Timestamp    time.Time `bigquery:"timestamp"`
	MigrationId  string    `bigquery:"migration_id"`
}
