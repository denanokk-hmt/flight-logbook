/*
======================
GCS
bucket::gyroscope
バケットファイルを取得
========================
*/
package extract

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	BQ "bwing.app/src/bigquery"
	TABLE "bwing.app/src/bigquery/table"
	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"
	"cloud.google.com/go/storage"
)

// Inerface
type ExtractGyroscopeLogRouter struct {
	BatchId      string
	ex           ExtractBucket
	parseCnt     int
	parseErrsCnt int
}
type ExtractGyroscopeMonthly struct {
	BatchId      string
	ex           ExtractBucket
	b            BUCKET.LogGyroscope
	pRecordss    []TABLE.GyroscopeImport
	parseCnt     int
	parseErrsCnt int
	parseErrsMsg []string
	mu           sync.RWMutex
}
type ExtractGyroscopeDaily struct {
	BatchId         string
	ex              ExtractBucket
	b               BUCKET.LogGyroscope
	pRecordss       []TABLE.GyroscopeImport
	parseCnt        int
	parseErrsCnt    int
	parseCntTTL     int
	parseErrsCntTTL int
	mu              sync.RWMutex
}

///////////////////////////////////////////////////
/* ===========================================
□ □ □ LogRouter □ □ □
※ログルータを廃止、過去データも削除しているため、復活した場合をみて、専用の処理をDailyに合わせた形で残す(2023.3.22)
GCS BucketからGyroscopeのログを取得
=========================================== */
func (e *ExtractGyroscopeLogRouter) ExtractGyroscope() (interface{}, int, error) {

	var err error

	/*------------------------------------------------
	GCSからバケットを取得
	------------------------------------------------*/

	//GCSバケットを取得
	extracts, err := e.ex.FetchBucket()
	if err != nil {
		return nil, 0, err
	}
	logss := extracts.(BUCKET.LogssStruct) //結果をCastして使い物に

	/*------------------------------------------------
	ログをパース
	------------------------------------------------*/

	cdt := time.Now()
	var load_results []ExtractBucketResults //結果箱の準備
	var pRecords []TABLE.GyroscopeImport    //パースしたすべてのログの箱

	//goroutine init
	ch := make(chan *CONFIG.Counter, len(logss.Objs))
	defer close(ch)
	var wg sync.WaitGroup
	wg.Add(len(logss.Objs))

	//取得したログをパース
	for i, logs := range logss.Objs {
		for ii, log := range logs {
			go func(i, ii int, log string) {
				defer wg.Done()

				//GCSログをパースして、BQへインサート出来るように整形
				fmt.Println("Parse bucket logging now:", logss.Paths[i][ii])
				l := &BUCKET.LogGyroscope{Path: logss.Paths[i][ii]}
				l.GyroscopeLogParserWithC(e.BatchId, log, &pRecords, ch)
				select {
				case v := <-ch:
					e.parseCnt += v.PCnt
					e.parseErrsCnt += v.PErrsCnt
				default:
					fmt.Println("no value")
				}
				//GCS 取得結果を格納
				var result ExtractBucketResults = ExtractBucketResults{
					Result:          CONFIG.RESULT_SUCCESS,
					Cdt:             cdt,
					LogNo:           i,
					LogPath:         logss.Paths[i][ii],
					LogDate:         logss.LogDate[i],
					ParseCount:      e.parseCnt,
					ParseErrorCount: e.parseErrsCnt,
				}
				fmt.Println(result)
				load_results = append(load_results, result)
			}(i, ii, log)
		}
	}
	wg.Wait()

	//BigQueryへログをロードする
	var bq BQ.LoadBqGyroscope
	iRecords, err := bq.Load2BqGyroscope(pRecords)
	if err != nil {
		return nil, 0, err
	}

	return load_results, iRecords, nil
}

///////////////////////////////////////////////////
/* ===========================================
□ □ □ Monthly □ □ □
※過去データ取得用
	基本的にローカル実行向けの仕様としている(1ファイルの容量が大きいことを想定)
	bucket関連のARGSを欲しいバケットの形に合わせること
	extract_monthをARGSに指定する
GCS BucketからGyroscopeのログを取得
=========================================== */
func (e *ExtractGyroscopeMonthly) ExtractGyroscope(term string) (interface{}, int, error) {

	cdt := time.Now()

	/*------------------------------------------------
	GCSからバケットファイルパスを取得
	※コマンドライン引数"bucket_paths:true"の場合、gyroscope_bucket_paths.jsonでから取得)
	------------------------------------------------*/

	//GCS clientを生成
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer gcsClient.Close()

	//GCSからバケットファイルパスを配列に格納
	var pathss []string
	if CONFIG.GetConfigArgs(CONFIG.ARGS_BUCKET_PATHS) == CONFIG.DEFAULT_STRING_VALUE_TRUE {
		pathss, err = e.ex.GetBucketPathsFromBucketPathsJson("gyroscope_bucket_paths.json")
	} else {
		pathss, err = e.ex.GetGyroscopeBucktPaths(gcsClient, term)
	}
	if err != nil {
		return nil, 0, err
	}

	/*------------------------------------------------
	準備(箱、並行処理、チャンクサイズ)
	------------------------------------------------*/
	var load_results []ExtractBucketResults //結果箱の準備
	var iRecordss int                       //BigQueryへのレコード挿入数

	//goroutine init
	ch := make(chan *CONFIG.Counter, len(pathss))
	defer close(ch)
	var wg sync.WaitGroup
	var index = 0
	var errGoroutine error

	//読み込むGCSのチャンクを計算
	var rss [][]string
	chunks := COMMON.ChunkCalculator2(len(pathss), CONFIG.DEFAULT_INT_VALUE_CHUNK_SIZE_GCS)
	for _, c := range chunks.Positions {
		rs := pathss[c.Start:c.End]
		rss = append(rss, rs)
	}

	/*------------------------------------------------
	過去ログをパース
	※chunkサイズにしたがってBucketパースを行っていく
	------------------------------------------------*/
	for _, rs := range rss {

		//初期化
		cdtParse2Insert := time.Now()
		wg.Add(len(rs))
		e.parseCnt = 0
		e.parseErrsCnt = 0
		e.pRecordss = nil

		//pathを読み込んで、パースしBqImportの箱に詰める
		for _, path := range rs {
			index++
			go func(index int, path string) {
				defer wg.Done()
				cdtParse := time.Now()
				fmt.Printf("Parse buckets now:[%s][%s][%d/%d]\n", path, e.ex.bucket, index, len(pathss))

				//Bucketのオブジェクト(ログデータ)を読み込む
				log, ctx, err := objectReaderPath(gcsClient, e.ex.bucket, path)
				if err != nil {
					fmt.Println(ctx.Err())
					errGoroutine = ctx.Err()
					return
				}
				if log == "" {
					return
				}

				//GCSログをパースして、BQへインサート出来るように整形
				e.b.Bucket = e.ex.bucket
				e.b.Path = path
				var pRecords []TABLE.GyroscopeImport //ロギングのパース結果の箱(Bq向け)
				err = e.b.GyroscopeOldLogParserWithC(e.BatchId, log, &pRecords, ch)
				if err != nil {
					fmt.Println(ctx.Err())
					return
				}

				//パース結果
				select {
				case v := <-ch:
					e.parseCnt += v.PCnt
					e.parseErrsCnt += v.PErrsCnt
					e.parseErrsMsg = append(e.parseErrsMsg, v.PErrsMsg...)
				default:
					fmt.Println("no value")
				}

				//パースしたレコードを格納(排他)
				e.mu.Lock()
				defer e.mu.Unlock()
				e.pRecordss = append(e.pRecordss, pRecords...)
				fmt.Printf("Parse buckets done:[%s][%s][%d/%d] time:[%f] parse:[%d]\n", path, e.ex.bucket, index, len(pathss), time.Since(cdtParse).Seconds(), e.parseCnt)
			}(index, path)

			//Goroutine上で起きたエラーハンドル
			if errGoroutine != nil {
				return nil, 0, errGoroutine
			}
		}
		wg.Wait()

		/*------------------------------------------------
		//BigQueryへログをロードする
		※chunkサイズにしたがってBucketパースを行っていく
		------------------------------------------------*/
		var bq BQ.LoadBqGyroscope
		iRecords, err := bq.Load2BqGyroscope(e.pRecordss)
		if err != nil {
			return nil, 0, err
		}
		iRecordss += iRecords //Insert累積数

		//GCSバケットを取得、BQへインサートした結果を格納
		var result ExtractBucketResults = ExtractBucketResults{
			Result:          CONFIG.RESULT_SUCCESS,
			Cdt:             cdt,
			LogNo:           index,
			LogPath:         "",
			ParseCount:      e.parseCnt,
			ParseErrorCount: e.parseErrsCnt,
		}

		//Sub results
		load_results = append(load_results, result)
		fmt.Printf("Done: Bucket:[%s][%d/%d] parse&InsertTime:[%f] parseSubTTL:[%d ]bqInsertSubTTL:[%d]\n", e.ex.bucket, index, len(pathss), time.Since(cdtParse2Insert).Seconds(), e.parseCnt, iRecords)
	}

	//TTL results
	fmt.Printf("log extract ALL done. time:[%f] bqTTL:[%d]\n", time.Since(cdt).Seconds(), iRecordss)
	return load_results, iRecordss, nil
}

///////////////////////////////////////////////////
/* ===========================================
□ □ □ Daily □ □ □
GCS BucketからGyroscopeのログを取得
=========================================== */
func (e *ExtractGyroscopeDaily) ExtractGyroscope(term string) (interface{}, int, error) {

	//GCS clientを生成
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer gcsClient.Close()

	/*------------------------------------------------
	GCSからバケットファイルパスを取得
	------------------------------------------------*/
	pathss, err := e.ex.GetGyroscopeBucktPaths(gcsClient, term)
	if err != nil {
		return nil, 0, err
	}

	/*------------------------------------------------
	ファイルログをパース、BigQueryへ
	------------------------------------------------*/
	var bq BQ.LoadBqGyroscope
	var iRecordss int                       //BigQueryへのレコード挿入数
	var load_results []ExtractBucketResults //結果箱の準備
	cdt := time.Now()

	//時間毎にたたみ直し
	mPathss, err := combineLogsByHourly(pathss)
	if err != nil {
		return nil, 0, err
	}

	//ログをパース、時間毎にログをサマライズし、BQへ挿入
	var mIdx int
	type results struct {
		parse    int
		bqInsert int
	}

	resultsMap := make(map[string]results)
	for suffix, pathss := range mPathss {

		//初期化
		var rSub results //Result count
		e.parseCnt = 0
		e.parseErrsCnt = 0
		e.pRecordss = nil
		mIdx += 1

		//goroutine init
		ch := make(chan *CONFIG.Counter, len(pathss))
		defer close(ch)
		var wg sync.WaitGroup
		wg.Add(len(pathss))

		for i, path := range pathss {
			go func(i int, path string) {
				defer wg.Done()

				cdtParse := time.Now()
				fmt.Printf("Suffix:[%s] Parse buckets now:[%s][%s][%d/%d]\n", suffix, path, e.ex.bucket, i+1, len(pathss))

				//Bucketのオブジェクト(ログデータ)を読み込む
				log, ctx, err := objectReaderPath(gcsClient, e.ex.bucket, path)
				if err != nil {
					fmt.Println(ctx.Err())
					//return nil, 0, err
				}

				//GCSログをパースして、BQへインサート出来るように整形
				e.b.Bucket = e.ex.bucket
				e.b.Path = path
				var pRecords []TABLE.GyroscopeImport //ロギングのパース結果の箱(Bq向け)
				err = e.b.GyroscopeLogParserWithC(e.BatchId, log, &pRecords, ch)
				if err != nil {
					fmt.Println(ctx.Err())
					//return nil, 0, err
				}
				select {
				case v := <-ch:
					e.parseCnt += v.PCnt
					e.parseErrsCnt += v.PErrsCnt
					e.parseCntTTL += v.PCnt
					e.parseErrsCntTTL += v.PErrsCnt
				default:
					fmt.Println("no value")
				}
				e.mu.Lock()
				defer e.mu.Unlock()
				e.pRecordss = append(e.pRecordss, pRecords...)
				fmt.Printf("Parse buckets done:[%s][%s][%d/%d] time:[%f] parse:[%d]\n", path, e.ex.bucket, i+1, len(pathss), time.Since(cdtParse).Seconds(), e.parseCnt)
			}(i, path)
		}
		wg.Wait()
		rSub.parse = len(e.pRecordss) //Parse sub count

		//BigQueryへログをロードする
		var iRecords int
		if len(e.pRecordss) != 0 {
			cdtBq := time.Now()
			iRecords, err = bq.Load2BqGyroscope(e.pRecordss)
			if err != nil {
				return nil, 0, err
			}
			fmt.Printf("Suffix:[%s] Bq insert done. time:[%f] insert:[%d]\n", suffix, time.Since(cdtBq).Seconds(), iRecords)
		}
		rSub.bqInsert = iRecords //Bq Insert count
		iRecordss += iRecords    //Insert累積数
		resultsMap[suffix] = rSub

		//GCSバケットを取得、BQへインサートした結果を格納
		var result ExtractBucketResults = ExtractBucketResults{
			Result:          CONFIG.RESULT_SUCCESS,
			Cdt:             cdt,
			LogNo:           mIdx,
			LogPath:         suffix,
			ParseCount:      e.parseCnt,
			ParseErrorCount: e.parseErrsCnt,
		}

		//Sub results
		load_results = append(load_results, result)
		fmt.Println(suffix, time.Since(cdt).Seconds())
		fmt.Printf("Done: Suffix:[%s] Bucket:[%s][%d/%d] parseSubTTL:[%d ]bqInsertSubTTL:[%d]\n", suffix, e.ex.bucket, mIdx, len(mPathss), rSub.parse, rSub.bqInsert)
	}

	fmt.Printf("log extract ALL done. time:[%f] bqTTL:[%d]\n", time.Since(cdt).Seconds(), iRecordss)
	return load_results, iRecordss, nil
}

///////////////////////////////////////////////////
/* ===========================================
PodName別に別れているログのパスをまとめる
=========================================== */
func combineLogsByHourly(pathss []string) (map[string][]string, error) {
	mapPaths := make(map[string][]string)
	for _, p := range pathss {
		path := strings.Split(p, "_")
		suffix := path[len(path)-1]
		suffix = strings.Replace(suffix, ".txt", "", 1)
		mapPaths[suffix] = append(mapPaths[suffix], p)
	}
	return mapPaths, nil
}

func checkDuplicateRecords(pRec *[]TABLE.GyroscopeImport) error {

	mapInsertIds := make(map[string]int)
	for _, r := range *pRec {
		mapInsertIds[r.InsertId] = 0
	}
	for k, m := range mapInsertIds {
		for _, r := range *pRec {
			if r.InsertId == k {
				m += 1
				mapInsertIds[k] = m
			}
		}
	}
	var nG []string
	for k, m := range mapInsertIds {
		if m > 1 {
			nG = append(nG, k)
		}
	}
	fmt.Println(nG)
	return nil
}
