/*
	=================================

GCS
bucket::flight_record
バケットの構造をここで指定する
バケットファイルをパースする
* =================================
*/
package bucket

import (
	"encoding/json"
	"time"

	TABLE "bwing.app/src/bigquery/table"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	GCS_BUCKET_NAME_FLIGHT_RECORD = "flight_record"
	GCS_MIDDLE_PATH_FLIGHT_RECORD = "stdout"
)

// ログ格納
type LogFlightRecord struct {
	Log  string
	Path string
}

// データロード結果
type LoadFlightRecordImportResults struct {
	Result  int
	Client  string
	Cdt     time.Time
	LogNo   int
	LogPath string
	LogDate string
	TTL     int
}

// データロード情報
type LoadFlightRecordImport struct {
	TimeStamp time.Time
}

// /////////////////////////////////////////
// GCSからバケットを取得し、JSON文字列をデコードするための箱
type JsonBucketFR struct {
	InsertId    string        `json:"insertId"`
	JsonPayload JsonPayloadFR `json:"jsonPayload"`
	Timestamp   string        `json:"timestamp"`
}
type JsonPayloadFR struct {
	Common       CommonFR       `json:"common"`
	Response     JsonResponseFR `json:"response"`
	Request      JsonRequestFR  `json:"request"`
	Session      JsonSessionFR  `json:"session"`
	Kind         string         `json:"kind"`
	CustomerUuid string         `json:"customer_uuid"`
	LogId        string         `json:"logiD"`
}
type CommonFR struct {
	WhatyaId     string `json:"hmt_id"`
	Type         string `json:"type"`
	LogiD        string `json:"logiD"`
	Api          string `json:"api"`
	Client       string `json:"client"`
	Service      string `json:"service"`
	Component    string `json:"component"`
	CustomerUuid string `json:"customer_uuid"`
	Use          string `json:"use"`
	SessionId    string `json:"session_id"`
}
type JsonResponseFR struct {
	Body      string `json:"body"`
	Headers   string `json:"headers"`
	Responded string `json:"responded"`
}
type JsonRequestFR struct {
	Body    string `json:"body"`
	Headers string `json:"headers"`
}
type JsonSessionFR struct {
	Token         string `json:"token"`
	Uid           string `json:"uid"`
	Rid           string `json:"rid"`
	OpSession     string `json:"op_session"`
	OpRid         string `json:"op_rid"`
	OpAccessToken string `json:"op_access_token"`
	OpCustUid     string `json:"op_cust_uid"`
	OpSystem      string `json:"op_system"`
	OpOpeUid      string `json:"op_ope_uid"`
}

///////////////////////////////////////////////////
/* ===========================================
// FlightRecordのログをパース
=========================================== */
func (l *LogFlightRecord) FlightRecordLogParser() ([]TABLE.FlightRecordImport, interface{}, error) {

	var err error
	var fs []TABLE.FlightRecordImport
	var cdt = time.Now()

	//1時間毎のログから、個別のログを分割(抽出)
	lds := SplitGcsLoggings2Array(l.Log)

	//パースエラーのinsertIdを格納する箱
	var parseErrs []string

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//jsonPayloadに紐づく文字列をUnmarshal
		var jb JsonBucketFR
		err = json.Unmarshal([]byte(ld), &jb)
		if err != nil {
			if jb.InsertId == "" {
				jb.InsertId = GcsLoggingExtractParseErrorNoInsertId(ld[0:50])
			}
			parseErrs = append(parseErrs, jb.InsertId)
			continue
		}

		//jsonPayload.commonを設定
		var common TABLE.CommonFR = TABLE.CommonFR{
			WhatyaId:     jb.JsonPayload.Common.WhatyaId,
			Type:         jb.JsonPayload.Common.Type,
			LogiD:        jb.JsonPayload.Common.LogiD,
			Api:          jb.JsonPayload.Common.Api,
			Client:       jb.JsonPayload.Common.Client,
			Service:      jb.JsonPayload.Common.Service,
			Component:    jb.JsonPayload.Common.Component,
			CustomerUuid: jb.JsonPayload.Common.CustomerUuid,
			Use:          jb.JsonPayload.Common.Use,
			SessionId:    jb.JsonPayload.Common.SessionId,
		}

		//jsonPayload.responseを設定
		var response TABLE.ResponseFR = TABLE.ResponseFR{
			Body:      jb.JsonPayload.Response.Body,
			Headers:   jb.JsonPayload.Response.Headers,
			Responded: jb.JsonPayload.Response.Responded,
		}

		//jsonPayload.requestを設定
		var request TABLE.RequestFR = TABLE.RequestFR{
			Body:    jb.JsonPayload.Request.Body,
			Headers: jb.JsonPayload.Request.Headers,
		}

		//jsonPayload.sessionを設定
		var session TABLE.SessionFR = TABLE.SessionFR{
			Token:         jb.JsonPayload.Session.Token,
			Uid:           jb.JsonPayload.Session.Uid,
			Rid:           jb.JsonPayload.Session.Rid,
			OpSession:     jb.JsonPayload.Session.OpSession,
			OpRid:         jb.JsonPayload.Session.OpRid,
			OpAccessToken: jb.JsonPayload.Session.OpAccessToken,
			OpCustUid:     jb.JsonPayload.Session.OpCustUid,
			OpSystem:      jb.JsonPayload.Session.OpSystem,
			OpOpeUid:      jb.JsonPayload.Session.OpOpeUid,
		}

		//Timesampを型変換
		ts, _ := time.Parse("2006-01-02T15:04:05Z07:00", jb.Timestamp)

		//Bq向けのレコードを作成
		var f TABLE.FlightRecordImport
		f.Cdt = cdt
		f.JsonPayload.Common = common
		f.JsonPayload.Response = response
		f.JsonPayload.Request = request
		f.JsonPayload.Session = session
		f.JsonPayload.Kind = jb.JsonPayload.Kind
		f.JsonPayload.CustomerUuid = jb.JsonPayload.CustomerUuid
		f.JsonPayload.LogiD = jb.JsonPayload.LogId
		f.Timestamp = ts

		//返却箱に格納
		fs = append(fs, f)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		return fs, nil, err
	}

	return fs, parseErrs, nil
}
