/*
=================================

サーバー起動時の起点ファイル
=================================
*/
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	ERR "bwing.app/src/error"
	GCS "bwing.app/src/gcs/extract"
	JOBS "bwing.app/src/jobs"
	LOGGING "bwing.app/src/log"
	"github.com/pkg/errors"
)

///////////////////////////////////////////////////
/* ===========================================
	args:Jobsの場合に利用
	key:value形式で記述
	1st(frequency), 2nd(contents)は必須
	2nd(contents)のvalueは、カンマつなぎ
	3rd意向は、指定ない場合、自動設定される
	抽出日は、指定なき場合、実行日の前日が設定される
	抽出日における入力ロケーションはJST
	コマンドライン引数の設定内容
	実行頻度			 【必須】[string]"frequency:daily" or "frequency:monthly"
	実行コンテンツ	【必須】[string]"contents:chaiend_tags_logging, flight_record,,,"
	強制的Index実行【任意】[string]"force_index:1"
	【任意】[string]"bucket_name:flight_record"
	【任意】[string]"bucket_middle_path:stdout"
	【任意】[string]"bucket_prefix:dev"
	【任意】[string]"bucket_suffix:"
	【任意】[string]"bucket_time_utc:false"
	【任意】[string]"bq_dataset_name:gcs_logging"
	【任意】[string]"bq_table_name:"flight_record"
	【任意】[string]"bq_no_insert:false"
	【任意】[string]"extract_start_date:2022/01/01"
	【任意】[string]"extract_end_date:2022/01/01"
* =========================================== */
func main() {
	/*------------------------------------------------
	共通前準備
	------------------------------------------------*/
	cdt := time.Now() //処理開始時間

	//実行されているタスクのIndexを取得
	taskNum := os.Getenv("CLOUD_RUN_TASK_INDEX")
	//attemptNum := os.Getenv("CLOUD_RUN_TASK_ATTEMPT")

	//実行するJobのIndexを決定
	index, _ := strconv.Atoi(taskNum)
	force_index := CONFIG.GetConfigArgs(CONFIG.ARGS_FORCE_INDEX)
	if force_index != "" {
		index, _ = strconv.Atoi(force_index) //強制的content実行
	}

	//実行するJobのcontentsを取得
	argsContents := CONFIG.GetConfigArgs(CONFIG.ARGS_CONTENTS)
	contents := (strings.Split(argsContents, ","))

	//contentを決定::タスクIndexの位置でcontentが切り換わる
	content := contents[index]

	/*------------------------------------------------
	ETL
	------------------------------------------------*/

	//バッチIDを発行
	batchId := COMMON.RandomString(10)

	//Jobを実行してログをETLする
	lr, rc, err := JOBS.ExecuteJobs(content, batchId)
	if err != nil {
		ERR.ErrorLoggingWithStackTrace(errors.WithStack(fmt.Errorf("[content:%s][Error:%w]", content, err)))
		log.Fatal(err)
		return
	}

	//Jobの結果を取りまとめる
	results := lr.([]GCS.ExtractBucketResults)
	bQty := len(results)                //取得バケット数
	exeSec := time.Since(cdt).Seconds() //処理時間計測
	responseOutput := fmt.Sprintf("【LOADED】[content:%s][bucketObjQty:%d][BqInsertQty:%d][ExeSec:%f]", content, bQty, rc, exeSec)

	//Jobの結果をロギング
	fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "JobReport", fmt.Sprintf("%+v", responseOutput)))
}
