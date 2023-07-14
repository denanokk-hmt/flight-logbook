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
	"fmt"
	"strings"
	"time"

	TABLE "bwing.app/src/bigquery/table"
	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	LOG "bwing.app/src/log"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	GCS_BUCKET_NAME_GYROSCOPE = "gyroscope"
	GCS_MIDDLE_PATH_GYROSCOPE = "events" //default path 変更したい場合、コマンドライン引数に指定
)

// ログ格納
type LogGyroscope struct {
	Bucket string
	Path   string
}

// データロード結果
type LoadGyroscopeImportResults struct {
	Result  int
	Client  string
	Cdt     time.Time
	LogNo   int
	LogPath string
	LogDate string
	TTL     int
}

// データロード情報
type LoadGyroscopeImport struct {
	TimeStamp time.Time
}

// /////////////////////////////////////////
// GCSからバケットを取得し、JSON文字列をデコードするための箱
type JsonBucketGy struct {
	InsertId        string `json:"insertId"`
	JsonTextPayload string `json:"textPayload"`
	Timestamp       string `json:"timestamp"`
}

/*
type JsonTextPayloadGy struct {
	Params ParamsGy `json:"Params"`
}
type ParamsGy struct {
	Host         string
	Url          string
	Path         string
	Ip           string
	UserAgent    string
	Referrer     string
	ClientId     string
	CustomerUuid string
	CustomerId   string
	WhatyaId     string
	CategoryId   string
	Type         string
	Id           string
	Label        string
	Value        string
	WyEvent      string
}
*/

///////////////////////////////////////////////////
/* ===========================================
// Gyroscopeのログをパース
	Dailyを基本としたGyroscopeサーバーから生まれたログ
=========================================== */
func (l *LogGyroscope) GyroscopeLogParserWithC(batchId, Log string, gss *[]TABLE.GyroscopeImport, ch chan *CONFIG.Counter) error {

	var err error
	var cdt = time.Now()
	var parseCnt, parseErrsCnt int
	var jsonPErr, notHaveParamsErr, notHaveTs int //検証用変数(本来は不要です)

	//timestamp異常の場合に設定するタイムスタンプ文字列
	sPath := strings.Split(l.Path, "/")
	sDTs := sPath[len(sPath)-1]
	sDTs = strings.Split(sDTs, "_")[0] + ".000000000Z"
	dTs, _ := time.Parse("2006-01-02T15:04:05.999999999Z07:00", sDTs)

	//バケットファイルを1行づつ、パースしレコードへ挿入できるように箱に詰め直し
	lp := strings.Replace(Log, "{\"insertId\":", "<|>{\"insertId\":", -1) //パイプで区切る
	lds := strings.Split(lp, "<|>")                                       //パイプをデリミタとして配列に分割
	lds = lds[1:]
	for i, ld := range lds {
		//fmt.Println("index is:", i)

		//「Body: Params: 」文字列(gyroscopeのログ仕様)を起点にログを切り出す
		bodyParts := strings.Split(ld, " Body: Params:")

		//書き損じの場合の考慮(エラーと認識させるが処理は次のパースへ続投)
		if len(bodyParts) != 2 {
			parseErrorLogging("Gyroscope log back parts Parse Errors", l.Bucket, l.Path, fmt.Sprintf("bucket file split error. row:[%d], err:[%s][%s]\n", i, "did not have ' Body: Params:' strings.", ld))
			parseErrsCnt++
			notHaveParamsErr++
			continue
		}

		//前半部分のパースをして、insertId(=gyroscopeで用意)を取得しておく
		//※普通にパースが失敗しても、Paramsが記録されているログが多く存在するため、ここでエラー判定はしない
		frontPart := bodyParts[0] + "\"}" //強引に切り出したので、お尻を締めてあげる
		var jb JsonBucketGy
		err = json.Unmarshal([]byte(frontPart), &jb) //jsonPayloadに紐づく文字列をUnmarshal
		if err != nil {
			if jb.InsertId == "" {
				jb.InsertId = GcsLoggingExtractParseErrorNoInsertId(ld[0:50])
			}
			//parseErrorLogging("Gyroscope log front parts Parse Errors", l.Bucket, l.Path, jb.InsertId) //普通にパース出来なかったことをロギング(エラーとはしない)
			parseErrsCnt++
			jsonPErr++
		}

		//パイプ文字で分割し、ロギングされたPostDataを個別に取得
		//(gyroscopeで、ここでSplitを行い易いようにパイプ区切りで出力させている=Json.Unmarshallよりも速度を優先)
		pArr := strings.Split(bodyParts[1], "|")

		//切り出したParamsをKey=Valueでマッピング
		paramsMapper := make(map[string]string)
		for _, p := range pArr {
			kv := strings.Split(p, "=")
			if len(kv) > 1 {
				paramsMapper[kv[0]] = strings.Join(kv[1:], "")
			}
		}

		//Ignore clients確認
		if !checkIgnoreClients(paramsMapper["client_id"]) {
			continue
		}

		//Timesampを型変換
		st := paramsMapper["timestamp"]
		if st == "" {
			//Paramsが取れている(欠損の可能性も大)が、timestampが無い場合
			parseErrorLogging("Gyroscope log Errors", l.Bucket, l.Path, fmt.Sprintf("no timestamp error. row:[%d], err:[%s]\n", i, ld))
			st = sDTs //デフォルトタイムスタンプ(文字列)
			notHaveTs++
		}
		ts, err := time.Parse("2006-01-02T15:04:05.999999999Z07:00", st)
		if err != nil {
			//timestampの値が異常だった場合
			ts = dTs //デフォルトタイムスタンプ
			notHaveTs++
		}

		//Bq向けのレコードを作成
		var gs TABLE.GyroscopeImport = TABLE.GyroscopeImport{
			BatchId:      batchId,
			Cdt:          cdt,
			InsertId:     jb.InsertId,
			Host:         paramsMapper["host"],
			Url:          paramsMapper["url"],
			Path:         paramsMapper["path"],
			Ip:           paramsMapper["ip"],
			UserAgent:    paramsMapper["user_agent"],
			Referrer:     paramsMapper["referrer"],
			ClientId:     paramsMapper["client_id"],
			CustomerUuid: paramsMapper["customer_uuid"],
			CustomerId:   paramsMapper["customer_id"],
			WhatyaId:     paramsMapper["hmt_id"],
			CategoryId:   paramsMapper["category_id"],
			Type:         paramsMapper["type"],
			Id:           paramsMapper["id"],
			Label:        paramsMapper["label"],
			Value:        paramsMapper["value"],
			WyEvent:      paramsMapper["wy_event"],
			Timestamp:    ts,
		}

		//返却箱に格納
		*gss = append(*gss, gs)
		parseCnt++
	}

	//パースエラー検証
	fmt.Printf("Parse Errors:: jsonPErr:[%d], notHaveParamsErr:[%d], notHaveTs:[%d]\n", jsonPErr, notHaveParamsErr, notHaveTs)
	ch <- &CONFIG.Counter{
		PCnt:     parseCnt,
		PErrsCnt: parseErrsCnt,
	}
	return nil
}

///////////////////////////////////////////////////
/* ===========================================
// Gyroscopeのログをパース
	MonthlyベースでGCSにバックアップされた2023/03以前のTDログ
=========================================== */
func (l *LogGyroscope) GyroscopeOldLogParserWithC(batchId, Log string, gss *[]TABLE.GyroscopeImport, ch chan *CONFIG.Counter) error {

	var cdt = time.Now()
	var parseCnt int
	var parseErrMsg []string
	var notHaveTs int //検証用変数(本来は不要です)

	//timestamp異常の場合に設定するタイムスタンプ文字列
	sPath := strings.Split(l.Path, "/")
	sDTs := sPath[len(sPath)-1]
	sDTs = strings.Split(sDTs, "_")[0] + ".000000000Z"
	dTs, _ := time.Parse("2006-01-02T15:04:05.999999999Z07:00", sDTs)

	//バケットファイルを1行づつ、パースしレコードへ挿入できるように箱に詰め直し
	lds := strings.Split(Log, "\n") //改行をデリミタとして配列に分割
	for i, ld := range lds {
		//fmt.Println("index is:", i)
		if ld == "" {
			continue
		}
		//CSVデータを配列化し、カラム毎にマッピング
		rArr, err := COMMON.ReadCSV(ld)
		if err != nil {
			fmt.Println("ERROR", err)
			errMsg := fmt.Sprintf("CSV Parse Error Bucket:[%s, %s] Row:[%d]", l.Bucket, l.Path, i)
			parseErrMsg = append(parseErrMsg, errMsg)
			continue
		}
		paramsMapper := make(map[string]string)
		paramsMapper["insert_id"] = "backup cdt:" + rArr[0]
		paramsMapper["host"] = rArr[1]
		paramsMapper["url"] = rArr[2]
		paramsMapper["path"] = rArr[3]
		paramsMapper["ip"] = rArr[4]
		paramsMapper["user_agent"] = rArr[5]
		paramsMapper["referrer"] = rArr[6]
		paramsMapper["client_id"] = rArr[7]
		paramsMapper["customer_uuid"] = rArr[8]
		paramsMapper["customer_id"] = rArr[9]
		paramsMapper["hmt_id"] = rArr[10]
		paramsMapper["category_id"] = rArr[11]
		paramsMapper["type"] = rArr[12]
		paramsMapper["id"] = rArr[13]
		paramsMapper["label"] = rArr[14]
		paramsMapper["value"] = rArr[15]
		paramsMapper["wy_event"] = rArr[16]
		paramsMapper["timestamp"] = rArr[17]

		//Ignore clients確認
		if !checkIgnoreClients(paramsMapper["client_id"]) {
			continue
		}

		//Timesampを型変換
		st := paramsMapper["timestamp"]
		st = strings.ReplaceAll(st, " UTC", "")
		st = strings.Replace(st, " ", "T", 1) + ".000000000Z"
		ts, err := time.Parse("2006-01-02T15:04:05.999999999Z07:00", st)
		if err != nil {
			ts = dTs //timestampの値が異常だった場合、デフォルトタイムスタンプ
			notHaveTs++
		}

		//Bq向けのレコードを作成
		var gs TABLE.GyroscopeImport = TABLE.GyroscopeImport{
			BatchId:      batchId,
			Cdt:          cdt,
			InsertId:     paramsMapper["insert_id"],
			Host:         paramsMapper["host"],
			Url:          paramsMapper["url"],
			Path:         paramsMapper["path"],
			Ip:           paramsMapper["ip"],
			UserAgent:    paramsMapper["user_agent"],
			Referrer:     paramsMapper["referrer"],
			ClientId:     paramsMapper["client_id"],
			CustomerUuid: paramsMapper["customer_uuid"],
			CustomerId:   paramsMapper["customer_id"],
			WhatyaId:     paramsMapper["hmt_id"],
			CategoryId:   paramsMapper["category_id"],
			Type:         paramsMapper["type"],
			Id:           paramsMapper["id"],
			Label:        paramsMapper["label"],
			Value:        paramsMapper["value"],
			WyEvent:      paramsMapper["wy_event"],
			Timestamp:    ts,
		}

		//返却箱に格納
		*gss = append(*gss, gs)
		parseCnt++
	}

	//パースエラー検証
	fmt.Printf("Parse Errors:: notHaveTs:[%d]\n", notHaveTs)
	ch <- &CONFIG.Counter{
		PCnt:     parseCnt,
		PErrsCnt: notHaveTs,
		PErrsMsg: parseErrMsg,
	}

	return nil
}

///////////////////////////////////////////////////
/* ===========================================
GCS Bucketの抽出時にパースエラーを起こしたファイルのinsertIdをWARNロギング
=========================================== */
func parseErrorLogging(logNm, bucket, path string, errMsg string) {

	//ログをJSONエンコード
	type err struct {
		bucket string
		path   string
		errMsg string
	}
	var parseErr err = err{
		bucket: bucket,
		path:   path,
		errMsg: errMsg,
	}

	//WARNロギング
	fmt.Println(LOG.SetLogEntry(LOG.WARN, logNm, fmt.Sprintf("%v\n", parseErr)))
}

///////////////////////////////////////////////////
/* ===========================================
Ignore clients確認
=========================================== */
func checkIgnoreClients(client string) bool {
	//コマンドライン引数で指定したクライアントIDと一致すればFalseで返却
	sClients := CONFIG.GetConfigArgs(CONFIG.ARGS_FORCE_IGNORE_CLIENT)
	clients := strings.Split(sClients, ",")
	for _, c := range clients {
		if c == client {
			return false
		}
	}
	return true
}
