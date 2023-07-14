/* =================================
BigQuery
Dataset::gcs_logging
Table::t_flight_record
テーブルの構造をここで指定する
* ================================= */
package table

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	DATASET_FLIGHT_RECORD = "gcs_logging"            //dataset
	TABLE_FRIGHT_RECORD   = "t_flight_record_import" //tableId
)

//データ挿入用テーブル定義
type FlightRecordImport struct {
	Cdt         time.Time     `bigquery:"Cdt"`
	JsonPayload JsonPayloadFR `bigquery:"jsonPayload"`
	Timestamp   time.Time     `bigquery:"timestamp"`
}
type JsonPayloadFR struct {
	Common       CommonFR   `bigquery:"common"`
	Response     ResponseFR `bigquery:"response"`
	Request      RequestFR  `bigquery:"request"`
	Session      SessionFR  `bigquery:"session"`
	Kind         string     `bigquery:"kind"`
	CustomerUuid string     `bigquery:"customer_uuid"`
	LogiD        string     `bigquery:"logiD"`
}
type CommonFR struct {
	WhatyaId     string `bigquery:"hmt_id"`
	Type         string `bigquery:"type"`
	LogiD        string `bigquery:"logiD"`
	Api          string `bigquery:"api"`
	Client       string `bigquery:"client"`
	Service      string `bigquery:"service"`
	Component    string `bigquery:"component"`
	CustomerUuid string `bigquery:"customer_uuid"`
	Use          string `bigquery:"use"`
	SessionId    string `bigquery:"session_id"`
}
type ResponseFR struct {
	Body      string `bigquery:"body"`
	Headers   string `bigquery:"headers"`
	Responded string `bigquery:"responded"`
}
type RequestFR struct {
	Body    string `bigquery:"body"`
	Headers string `bigquery:"headers"`
}
type SessionFR struct {
	Token         string `bigquery:"token"`
	Uid           string `bigquery:"uid"`
	Rid           string `bigquery:"rid"`
	OpSession     string `bigquery:"op_session"`
	OpRid         string `bigquery:"op_rid"`
	OpAccessToken string `bigquery:"op_access_token"`
	OpCustUid     string `bigquery:"op_cust_uid"`
	OpSystem      string `bigquery:"op_system"`
	OpOpeUid      string `bigquery:"op_ope_uid"`
}
