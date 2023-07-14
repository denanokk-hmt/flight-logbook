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
	DATASET_MAU_COUNTER = "gcs_logging"                  //dataset
	TABLE_MAU_COUNTER   = "t_active_user_counter_import" //tableId
)

// データ挿入用テーブル定義
type MauCounterImport struct {
	Client    string    `bigquery:"client"`
	Count     int64     `bigquery:"count"`
	Cdt       time.Time `bigquery:"cdt"`
	Month     string    `bigquery:"month"`
	Type      string    `bigquery:"type"`
	Timestamp time.Time `bigquery:"timestamp"`
}
