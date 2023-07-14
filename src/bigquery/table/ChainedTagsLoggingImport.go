/*
	=================================

BigQuery
Dataset::gcs_logging
Table::t_chained_tags_logging_import
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
	DATASET_CHAINED_TAGS_LOGGING = "gcs_logging"                   //dataset
	TABLE_CHAINED_TAGS_LOGGING   = "t_chained_tags_logging_import" //tableId
)

type ChainedTagsLoggingImport struct {
	dataset string
	table   string
}

func (c ChainedTagsLoggingImport) ToString() (string, string) {
	c.dataset = DATASET_CHAINED_TAGS_LOGGING
	c.table = TABLE_CHAINED_TAGS_LOGGING
	return c.dataset, c.table
}

/*
//GCSバケットから書き出す先のテーブル定義
type ChainedTagsLoggingImport struct {
	Pdt              time.Time
	Cdt              time.Time
	ClientId         string
	CustomerUuid     string
	WhatYaId         string
	ATID             string
	DDID             int
	RelatedUnixtime  int
	PublishedAt      string
	RelatedWordsLog  [][]string //slice of slice
	SelectedItemId   string
	SelectedItemName string
}
*/

// BigQueryへのInsert用のテーブル定義
type ChainedTagsLoggingImportInsert struct {
	Pdt              time.Time `bigquery:"timestamp"`
	Cdt              time.Time `bigquery:"cdt"`
	ClientId         string    `bigquery:"client_id"`
	CustomerUuid     string    `bigquery:"customer_uuid"`
	WhatYaId         string    `bigquery:"hmt_id"`
	ATID             string    `bigquery:"atid"`
	DDID             int       `bigquery:"ddid"`
	GID              string    `bigquery:"gid"`  //[unixtime]_[index] :GroupID
	GQty             int       `bigquery:"gqty"` //:GroupIDの個数
	GSort            int       `bigquery:"g_sort"`
	RelatedUnixtime  int       `bigquery:"related_unixtime"`
	PublishedAt      string    `bigquery:"published_at"`
	RelatedWordsLog  []string  `bigquery:"related_words_log"`
	SelectedItemId   string    `bigquery:"selected_item_id"`
	SelectedItemName string    `bigquery:"selected_item_name"`
}
